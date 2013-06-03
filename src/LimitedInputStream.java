
package baldrickv.s3streamingtool;

import java.io.InputStream;

public class LimitedInputStream extends InputStream
{
    private InputStream base;
    private long start;
    private long count;
    private long lastRead;
    private double maxBytesPerNS;

    private boolean applyLimit;
    public LimitedInputStream(InputStream base, double maxBytesPerSecond)
    {
        this.base = base;
        maxBytesPerNS = maxBytesPerSecond / 1e9;

        applyLimit=false;


    }

    @Override
    public int available()
        throws java.io.IOException
    {
        //return (int)Math.min(base.available(), getAllow());
        return base.available();
    }

    @Override
    public void close()
        throws java.io.IOException
    {
        base.close();
    }

    @Override
    public long skip(long n)
        throws java.io.IOException
    {
        return base.skip(n);
    }

    @Override
    public boolean markSupported()
    {
        return base.markSupported();
    }
    
    @Override
    public void mark(int readlimit)
    {
        base.mark(readlimit);
    }

    @Override
    public void reset()
        throws java.io.IOException
    {
        base.reset();

        //AWS Client seems to read through once to build signature
        //and then read a second time to actually send it.
        //So we only limit on the secnod pass, after the reset
        applyLimit=true;
    }
        

    private long getAllow()
    {

        long now = System.nanoTime();
        double delta = now - start;
        double allow = delta * maxBytesPerNS;
        return (long)allow;
    }

    @Override 
    public int read()
        throws java.io.IOException
    {
        if (!applyLimit)
        {
            return base.read();
        }

        if (start==0) start=System.nanoTime();

        while(getAllow() <= count)
        {
            try
            {
                Thread.sleep(5);
            }
            catch(Exception e)
            {
                throw new java.io.IOException(e);
            }
        }
        count++;
        return base.read();

    }
    

}
