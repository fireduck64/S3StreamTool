package baldrickv.s3streamingtool;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.util.List;
import java.util.LinkedList;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class StorageS3 extends StorageInterface
{
    private AmazonS3Client s3;
    public StorageS3(AmazonS3Client s3)
    {
        this.s3 = s3;
    }

    public String initiateMultipartUpload(String bucket, String file, int part_size)
    {
        InitiateMultipartUploadRequest init_req = new InitiateMultipartUploadRequest(bucket, file);

        return s3.initiateMultipartUpload(init_req).getUploadId();
    }

    public String uploadPartActual(String bucket, String file, String upload_id, String checksum, int part_size, int part_number, byte[] out, double max_rate)
    {
        int size = out.length;

        UploadPartRequest req = new UploadPartRequest();

        req.setBucketName(bucket);
        req.setKey(file);
        req.setUploadId(upload_id);
        req.setPartNumber(part_number);

        req.setPartSize(size);

        String md5 = Hash.getMd5ForS3(out);
        req.setMd5Digest(md5);

        if (max_rate <= 0)
        {
            req.setInputStream(new ByteArrayInputStream(out));
        }
        else
        {
            req.setInputStream(new LimitedInputStream(new ByteArrayInputStream(out), max_rate));
        }


        return s3.uploadPart(req).getPartETag().getETag();

         
    }

    public String completeMultipartupload(String bucket, String file, String upload_id, long total_len, List<String> parts)
    {
        LinkedList<PartETag> tags = new LinkedList<PartETag>();
        int n =1;
        for(String t : parts )
        {
            tags.add(new PartETag(n, t));
            n++;
        }
        CompleteMultipartUploadRequest complete_req = new CompleteMultipartUploadRequest(bucket, file, upload_id, tags);
        return s3.completeMultipartUpload(complete_req).getETag();


    }


    /**
     * S3 uses an inclusive range (on both ends).
     * This call however uses a more standard, inclusive on the start and exclusive on the end.
     * so returns all the bytes X such that (start <= X < end)
     */
    public byte[] downloadPartActual(String bucket, String file, long start, long end)
        throws java.io.IOException
    {
        GetObjectRequest req = new GetObjectRequest(bucket, file);

        req.setRange(start, end - 1);
        S3Object obj = s3.getObject(req);

        int len = (int)obj.getObjectMetadata().getContentLength();
        byte[] b=new byte[len];
        DataInputStream din = new DataInputStream(obj.getObjectContent());

        din.readFully(b);
        din.close();

        return b;

    }

}
