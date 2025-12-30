
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";


export const handler = async (event) => {


    const envBucket = process.env.LANDING_BUCKET;
    const project = process.env.PROJECT;
    const bucketRegion = process.env.BUCKET_REGION;

    const s3 = new S3Client({ region: bucketRegion });
    console.log ("Bucket: " + envBucket + " project: " + project );

    const bucket = envBucket;
    const prefix = `${project}/dbo`;
    const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 }));
    const ok = (res.KeyCount ?? 0) > 0;
    if (!ok) throw new Error(`No input objects found in s3://${bucket}/${project}/${prefix}. Upload a file and retry.`);

  return { ok, bucket, bucket, prefix, envBucket};
};
