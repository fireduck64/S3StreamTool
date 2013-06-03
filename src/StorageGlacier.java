package baldrickv.s3streamingtool;

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.*;
import com.amazonaws.services.glacier.TreeHashGenerator;

import org.apache.commons.codec.binary.Hex;

import java.util.List;
import java.util.LinkedList;
import java.io.ByteArrayInputStream;

public class StorageGlacier extends StorageInterface
{
    private AmazonGlacierClient glacier;
    public StorageGlacier(AmazonGlacierClient glacier)
    {
        this.glacier = glacier;
    }

    public String initiateMultipartUpload(String bucket, String file, int part_size)
    {
        InitiateMultipartUploadRequest init_req = new InitiateMultipartUploadRequest(bucket, file, "" + part_size);

        return glacier.initiateMultipartUpload(init_req).getUploadId();
    }

    public String uploadPartActual(String bucket, String file, String upload_id, String checksum, int part_size, int part_number, byte[] out, double max_rate)
    {
        int size = out.length;

        UploadMultipartPartRequest req = new UploadMultipartPartRequest();

        req.setVaultName(bucket);
        req.setUploadId(upload_id);
        req.setChecksum(checksum);
        long part_size_long = part_size;
        long part_number_long = part_number;

        long start = part_size_long * (part_number_long - 1L);
        long end = start+size-1L;
        req.setRange("bytes " + start+"-"+end+"/*");

        if (max_rate <= 0)
        {
            req.setBody(new ByteArrayInputStream(out));
        }
        else
        {
            req.setBody(new LimitedInputStream(new ByteArrayInputStream(out), max_rate));
        }

        return glacier.uploadMultipartPart(req).getChecksum();



         
    }

    public String completeMultipartupload(String bucket, String file, String upload_id, long total_len, List<String> parts)
    {

        
        LinkedList<byte[]> tags = new LinkedList<byte[]>();
        int n =1;
        for(String t : parts )
        {
            try
            {
                byte[] b = Hex.decodeHex(t.toCharArray());
                tags.add(b);
            }
            catch(org.apache.commons.codec.DecoderException e)
            {
                throw new RuntimeException(e);
            }
        }
        String checksum = TreeHashGenerator.calculateTreeHash(tags);

        CompleteMultipartUploadRequest req = new CompleteMultipartUploadRequest(bucket, upload_id, "" + total_len, checksum);

        CompleteMultipartUploadResult res = glacier.completeMultipartUpload(req);
        System.out.println("Location: " + res.getLocation());
        System.out.println("Archive ID: " + res.getArchiveId());


        return glacier.completeMultipartUpload(req).getLocation();


    }

    public byte[] downloadPartActual(String bucket, String file, long start, long end)
        throws java.io.IOException
    {
        return null;
    }


}
