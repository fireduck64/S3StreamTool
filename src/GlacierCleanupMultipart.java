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

import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.*;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.List;
import java.util.Scanner;

public class GlacierCleanupMultipart
{

	public static void cleanup(S3StreamConfig config)
		throws Exception
	{

        AmazonGlacierClient glacier = config.getGlacierClient();
		String bucket = config.getS3Bucket();

		ListMultipartUploadsRequest list_req = new ListMultipartUploadsRequest(bucket);

		List<UploadListElement> list = glacier.listMultipartUploads(list_req).getUploadsList();

		Scanner scan = new Scanner(System.in);

		for(UploadListElement mu : list)
		{
			System.out.println("-----------------------");
			System.out.println("  bucket: " + bucket);
			System.out.println("  desc: " + mu.getArchiveDescription());
			System.out.println("  uploadId: " + mu.getMultipartUploadId() );
			System.out.println("  initiated at: " + mu.getCreationDate());
			System.out.println("-----------------------");

			System.out.print("Abort this upload [y|N]? ");
			String result = scan.nextLine().trim().toLowerCase();
			if (result.equals("y"))
			{
				AbortMultipartUploadRequest abort = new AbortMultipartUploadRequest(bucket, mu.getMultipartUploadId());

				glacier.abortMultipartUpload(abort);
				System.out.println("Aborted upload");
			}
			else
			{
				System.out.println("Leaving this one alone");

			}



		}






	}


	

}
