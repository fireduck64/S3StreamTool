package baldrickv.s3streamingtool;

import java.text.DecimalFormat;


import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.List;

public abstract class StorageInterface
{
    protected static Logger log = Logger.getLogger(StorageInterface.class.getName());

    public abstract String initiateMultipartUpload(String bucket, String file, int part_size);

    public String uploadPart(String bucket, String file, String upload_id, String checksum, int part_size, int part_number, byte[] out, double max_rate)
    {
        log.log(Level.FINE, "Started " + file + "-" + part_number);
        int size = out.length;

        long t1 = System.nanoTime();

        String tag = null;
        while(tag==null)
        {

            try
            {
                tag = uploadPartActual(bucket, file, upload_id, checksum, part_size, part_number, out, max_rate);
            }
            catch(Throwable t)
            {
                t.printStackTrace();
                try{Thread.sleep(10000);}catch(Exception e){}
            }
        }

        long t2 = System.nanoTime();

        double seconds = (double)(t2 - t1) / 1000000.0 / 1000.0;
        double rate = (double)size / seconds / 1024.0;

        DecimalFormat df = new DecimalFormat("0.00");

        log.log(Level.FINE,file + "-" + part_number + " size: " + size + " in " + df.format(seconds) + " sec, " + df.format(rate) + " kB/s");
        System.out.println(file + "-" + part_number + " size: " + size + " in " + df.format(seconds) + " sec, " + df.format(rate) + " kB/s");
        
        return tag;
    }


    public abstract String uploadPartActual(String bucket, String file, String upload_id, String checksum, int part_size, int part_number, byte[] out, double max_rate);

    public abstract String completeMultipartupload(String bucket, String file, String upload_id, long total_len, List<String> parts);

    public String getFileDownloadKey(S3StreamConfig config)
    {
        return config.getS3File();
    }

    public abstract long getObjectSize(S3StreamConfig config);



    /**
     * S3 uses an inclusive range (on both ends).
     * This call however uses a more standard, inclusive on the start and exclusive on the end.
     * so returns all the bytes X such that (start <= X < end)
     */
    public byte[] downloadPart(String bucket, String file, long start, long end)
    {
        long t1 = System.nanoTime();
        while(true)
        {   
            try
            {   
                log.log(Level.FINE, "Started " + file + " " + start + " " + end);

                byte[] b = downloadPartActual(bucket, file, start, end);


                int len = b.length;
                long t2 = System.nanoTime();

                double seconds = (double)(t2 - t1) / 1000000.0 / 1000.0;
                double rate = (double)len / seconds / 1024.0;

                DecimalFormat df = new DecimalFormat("0.00");

                log.log(Level.FINE,file + " " + start + " " + end  + " size: " + len + " in " + df.format(seconds) + " sec, " + df.format(rate) + " kB/s");

                return b;



            }
            catch(DelayException t)
            {
                log.log(Level.WARNING, "Download not ready " + t.toString());
                try{Thread.sleep(120000);}catch(Exception e){}

            }
            catch(Throwable t)
            {   
                log.log(Level.WARNING, "Error in download", t);
                try{Thread.sleep(5000);}catch(Exception e){}
            }
        }




    }

    public abstract byte[] downloadPartActual(String bucket, String file, long start, long end)
        throws java.io.IOException;


}
