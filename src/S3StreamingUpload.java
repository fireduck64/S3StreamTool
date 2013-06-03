/*
Copyright (c) 2010 baldrickv

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/


package baldrickv.s3streamingtool;

import com.amazonaws.auth.AWSCredentials;


import java.util.Random;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.DecimalFormat;

import javax.crypto.Cipher;
import java.security.Key;
import javax.crypto.spec.IvParameterSpec;
import java.nio.ByteBuffer;

import java.util.logging.Logger;
import java.util.logging.Level;

import com.amazonaws.services.s3.AmazonS3Client;

public class S3StreamingUpload
{
	protected static Logger log = Logger.getLogger(S3StreamingUpload.class.getName());


    public static void main(String args[])
		throws Exception
    {
		if (args.length != 5)
		{
			System.out.println("Args: S3StreamingUpload bucket s3_file_name block_size key_path aws_creds_path");
			System.exit(-1);
		}

		String bucket = args[0];
		String file = args[1];
		int block_size = Integer.parseInt(args[2]);
		String key_path = args[3];
		String aws_creds_path = args[4];

		Logger.getLogger("").setLevel(Level.WARNING);
		Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).setLevel(Level.INFO);
		log.setLevel(Level.INFO);

		Key secret_key = Utils.loadSecretKey(key_path);
		AWSCredentials creds = Utils.loadAWSCredentails(aws_creds_path);

		upload(System.in, bucket, file, block_size, secret_key, creds);

    }

    public static void upload(InputStream in, String bucket, String file, int block_size, Key secret_key, AWSCredentials creds)
        throws Exception
    {   
        S3StreamConfig config = new S3StreamConfig();
        config.setInputStream(in);
        config.setS3Bucket(bucket);
        config.setS3File(file);
        config.setBlockSize(block_size);
        config.setSecretKey(secret_key);
        config.setEncryption(true);
        config.setS3Client(new AmazonS3Client(creds));

        upload(config);
    }


	public static void upload(S3StreamConfig config)
		throws Exception
	{
		
		long start_time = System.currentTimeMillis();

		InputStream in = config.getInputStream();

		Random rnd = new Random();

		String bucket = config.getS3Bucket();
		String file = config.getS3File();

		Cipher cipher = null;
		int block_size = config.getBlockSize();

		int block_no = 1; //Have to start with 1 for S3 Multipart


		byte[] iv=null;

		if (config.getEncryption())
		{
			Key secret_key = config.getSecretKey();

			int key_size = secret_key.getEncoded().length;
			cipher = Cipher.getInstance(config.getEncryptionMode());

			iv=new byte[key_size];
			rnd.nextBytes(iv);
			IvParameterSpec iv_spec = new IvParameterSpec(iv);
			cipher.init(Cipher.ENCRYPT_MODE, secret_key, iv_spec);
		}

		if (block_size < 5242880)
		{
			log.severe("Block size set to " + block_size + ". S3 will likely reject multipart uploads of block size less than 5MB (5242880)");
			log.severe("Unless they have changed their policy this upload will fail at the final completeMultipartUpload step");
			log.severe("Unless upload fits in one block, in which case it will be fine");
			
		}

		boolean first_block = true;
		boolean last_block = false;

        String upload_id = config.getStorageInterface().initiateMultipartUpload(bucket, file, block_size);


		Semaphore read_allow = new Semaphore(config.getIOThreads());

		BlockWeaver<String> etags = new BlockWeaver<String>(1);

		LinkedBlockingQueue<DataBlock> save_queue = new LinkedBlockingQueue<DataBlock>();


		LinkedList<S3StreamingUploadThread> threads = new LinkedList<S3StreamingUploadThread>();
		for(int i=0; i< config.getIOThreads(); i++)
		{
			S3StreamingUploadThread t = new S3StreamingUploadThread(
				upload_id, 
				config,
				read_allow,
				etags,
				save_queue);
			t.start();
			threads.add(t);
		}
		long total_size = 0;


		while(!last_block)
		{
			read_allow.acquire();

			int next_block_size = block_size;
			if ((first_block) && (cipher != null))
			{
				next_block_size -= iv.length;
			}
			byte[] plain = readNextBlock(in, next_block_size);


			byte[] out = null;

			if (plain.length != next_block_size)
			{
				last_block = true;
				if (cipher != null)
				{
					out = cipher.doFinal(plain);
				}
				else
				{
					out = plain;
				}

			}
			else
			{
				if (cipher != null)
				{
					out = cipher.update(plain);
				}
				else
				{
					out = plain;
				}
			}
			plain=null; //Don't need it anymore, make it GC-able

			if ((first_block) && (cipher != null))
			{
				byte[] out2 = out;

				out = new byte[iv.length + out2.length];
				ByteBuffer bb = ByteBuffer.wrap(out);
				bb.put(iv);
				bb.put(out2);
				first_block=false;
			}
            if (out.length==0) break;
            
            total_size += out.length;

			save_queue.put(new DataBlock(block_no, out));

			//s3.model.PartETag tag = put(s3, bucket, file, upload_id, block_no, out);
			//parts.add(tag);

			block_no++;
		}

		
		LinkedList<String> parts = new LinkedList<String>();
		for(int i=1; i<block_no; i++)
		{
            String part = etags.getNextBlock();
			parts.add(part);
		}

		for(S3StreamingUploadThread t : threads)
		{
			t.interrupt();
			t.join();
		}
        String etag = config.getStorageInterface().completeMultipartupload(bucket, file, upload_id, total_size, parts);

		log.info("Uploaded " + bucket + " " + file + " with etag " + etag); 

   		long end_time = System.currentTimeMillis();

		double seconds = (double)(end_time - start_time)/1000.0;
		double rate = (double)total_size / seconds;
		double rate_kb = rate / 1024.0;
		DecimalFormat df = new DecimalFormat("0.00");

		log.info("Uploaded " + total_size + " at rate of " + df.format(rate_kb) + " kB/s");
		

	}


	
	private static byte[] readNextBlock(InputStream in, int max_block_size)
		throws Exception
	{
		byte buffer[]=new byte[max_block_size];
		int idx =0;

		while(idx < max_block_size)
		{
			int r = in.read(buffer, idx, max_block_size - idx);
			if (r == -1)
			{
				byte b2[]=new byte[idx];
				for(int i=0; i<idx; i++)
				{
					b2[i] = buffer[i];
				}
				return b2;
			}
			idx += r;
		}
		return buffer;

	}


}
