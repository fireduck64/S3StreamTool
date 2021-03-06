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

import java.io.InputStream;
import java.io.OutputStream;

import java.security.Key;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.glacier.AmazonGlacierClient;


public class S3StreamConfig
{
	public static final int DEFAULT_BLOCK_MB =100; //100mb
	public static final int DEFAULT_IO_THREADS = 2;
	public static final String DEFAULT_ENCRYPTION_MODE = "AES/CBC/PKCS5PADDING";


    private AmazonGlacierClient glacier_client;
	private AmazonS3Client s3_client;
    private StorageInterface storage_interface;
	private String s3_bucket;
	private String s3_file;

    private boolean glacier=false;
    private String glacier_job_id=null;

	private boolean encryption=false;
	private String encryption_mode= DEFAULT_ENCRYPTION_MODE;
	private Key secret_key;

	private int block_size=DEFAULT_BLOCK_MB * 1048576;
	private int io_threads=DEFAULT_IO_THREADS;

    private double max_bytes_per_second=0.0; //Per thread

	private InputStream source; //Only applies to upload
	private OutputStream destination; //Only applies to download

	public void setS3Client(AmazonS3Client c){s3_client = c;}
    public void setGlacierClient(AmazonGlacierClient c){glacier_client = c;}
    public void setStorageInterface(StorageInterface i){storage_interface = i;}
	public void setS3Bucket(String s){s3_bucket = s;}
	public void setS3File(String s){s3_file = s;}
	public void setEncryption(boolean e){encryption = e;}
	public void setEncryptionMode(String m){encryption_mode = m;}
	public void setSecretKey(Key k){secret_key = k;}
	public void setBlockSize(int z){block_size = z;}
	public void setIOThreads(int n){io_threads = n;}
    public void setMaxBytesPerSecond(double m){max_bytes_per_second = m;}

	public void setInputStream(InputStream in){source = in;}
	public void setOutputStream(OutputStream out){destination = out;}
    public void setGlacier(boolean g){glacier = g;}
    public void setGlacierJobId(String job_id){glacier_job_id = job_id;}


	public AmazonS3Client getS3Client(){return s3_client;}
    public AmazonGlacierClient getGlacierClient(){return glacier_client;}
    public StorageInterface getStorageInterface()
    {
        if (storage_interface == null)
        {
            storage_interface = new StorageS3(getS3Client());
        }
        return storage_interface;
    }
	public String getS3Bucket(){return s3_bucket;}
	public String getS3File(){return s3_file;}
	public boolean getEncryption(){return encryption;}
	public String getEncryptionMode(){return encryption_mode;}
	public Key getSecretKey(){return secret_key;}
	public int getBlockSize(){return block_size;}
	public int getIOThreads(){return io_threads;}
    public boolean getGlacier(){return glacier;}
    public String getGlacierJobId(){return glacier_job_id;}
    public double getMaxBytesPerSecond(){ return max_bytes_per_second;}

	public InputStream getInputStream(){return source;}
	public OutputStream getOutputStream(){return destination;}



}
