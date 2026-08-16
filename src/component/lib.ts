import { action, mutation, query } from "./_generated/server.js";
import { v } from "convex/values";
import schema from "./schema.js";
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import {
  createR2Client,
  paginationReturnValidator,
  r2ConfigValidator,
} from "../shared.js";
import type { Doc, TableNames } from "./_generated/dataModel.js";
import { api, components } from "./_generated/api.js";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { asyncMap } from "convex-helpers";
import { paginator } from "convex-helpers/server/pagination";
import { ActionRetrier } from "@convex-dev/action-retrier";
import type { R2Callbacks } from "../client/index.js";

const DEFAULT_LIST_LIMIT = 100;
type ActionRetrierComponent = ConstructorParameters<typeof ActionRetrier>[0];

// The component is registered in convex.config.ts. Keep the generated API file
// untouched and derive the nested component type from ActionRetrier itself.
const retrier = new ActionRetrier(
  (components as { actionRetrier: ActionRetrierComponent }).actionRetrier,
);

const getUrl = async (r2: S3Client, bucket: string, key: string) => {
  return await getSignedUrl(
    r2,
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
};

export const store = action({
  args: {
    ...r2ConfigValidator.fields,
    url: v.string(),
  },
  handler: async (_ctx, args) => {
    const r2 = createR2Client(args);
    const response = await fetch(args.url);
    const blob = await response.blob();
    const key = crypto.randomUUID();
    const command = new PutObjectCommand({
      Bucket: args.bucket,
      Key: key,
      Body: blob,
      ContentType: response.headers.get("Content-Type") ?? undefined,
    });
    await r2.send(command);
    return key;
  },
});

export const getMetadata = query({
  args: {
    key: v.string(),
    ...r2ConfigValidator.fields,
  },
  returns: v.union(
    v.object({
      ...schema.tables.metadata.validator.fields,
      url: v.string(),
      bucketLink: v.string(),
    }),
    v.null(),
  ),
  handler: async (ctx, args) => {
    const { key, ...r2Config } = args;
    const r2 = createR2Client(r2Config);
    const metadata = await ctx.db
      .query("metadata")
      .withIndex("bucket_key", (q) =>
        q.eq("bucket", args.bucket).eq("key", args.key),
      )
      .unique();
    if (!metadata) {
      return null;
    }
    return {
      ...withoutSystemFields(metadata),
      url: await getUrl(r2, r2Config.bucket, key),
      bucketLink: metadata.link.replace(/\/objects\/.*\/details$/, ""),
    };
  },
});

export const listMetadata = query({
  args: {
    limit: v.optional(v.number()),
    cursor: v.optional(v.string()),
    ...r2ConfigValidator.fields,
  },
  returns: paginationReturnValidator(
    v.object({
      ...schema.tables.metadata.validator.fields,
      url: v.string(),
      bucketLink: v.string(),
    }),
  ),
  handler: async (ctx, args) => {
    const { limit, cursor, ...r2Config } = args;
    const r2 = createR2Client(r2Config);
    const results = await paginator(ctx.db, schema)
      .query("metadata")
      .withIndex("bucket", (q) => q.eq("bucket", r2Config.bucket))
      .paginate({
        numItems: limit ?? DEFAULT_LIST_LIMIT,
        cursor: cursor ?? null,
      });
    return {
      ...results,
      page: await asyncMap(results.page, async (doc) => ({
        ...withoutSystemFields(doc),
        url: await getUrl(r2, r2Config.bucket, doc.key),
        bucketLink: doc.link.replace(/\/objects\/.*$/, ""),
      })),
    };
  },
});

export const upsertMetadata = mutation({
  args: schema.tables.metadata.validator.fields,
  returns: v.object({
    isNew: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const existingMetadata = await ctx.db
      .query("metadata")
      .withIndex("bucket_key", (q) =>
        q.eq("bucket", args.bucket).eq("key", args.key),
      )
      .unique();
    if (existingMetadata) {
      await ctx.db.patch("metadata", existingMetadata._id, {
        contentType: args.contentType,
        size: args.size,
        sha256: args.sha256,
        lastModified: args.lastModified,
        link: args.link,
      });
      return { isNew: false };
    }
    await ctx.db.insert("metadata", {
      key: args.key,
      contentType: args.contentType,
      size: args.size,
      sha256: args.sha256,
      bucket: args.bucket,
      lastModified: args.lastModified,
      link: args.link,
    });
    return { isNew: true };
  },
});

export const syncMetadata = action({
  args: {
    key: v.string(),
    onComplete: v.optional(v.string()),
    ...r2ConfigValidator.fields,
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const { key, ...r2Config } = args;
    const r2 = createR2Client(r2Config);
    const command = new HeadObjectCommand({
      Bucket: r2Config.bucket,
      Key: key,
    });
    const response = await r2.send(command);

    const accountId = /\/{2}([^/.]+)\./.exec(r2Config.endpoint)?.[1] ?? "";
    const link = `https://dash.cloudflare.com/${accountId}/r2/default/buckets/${r2Config.bucket}/objects/${key}/details`;
    const { isNew } = await ctx.runMutation(api.lib.upsertMetadata, {
      key,
      lastModified: response.LastModified?.toISOString() ?? "",
      contentType: response.ContentType,
      size: response.ContentLength,
      sha256: response.ChecksumSHA256,
      bucket: r2Config.bucket,
      link,
    });
    const onComplete = args.onComplete as R2Callbacks["onSyncMetadata"];
    if (onComplete) {
      await ctx.runMutation(onComplete, {
        key,
        bucket: r2Config.bucket,
        isNew,
      });
    }
  },
});

export const deleteMetadata = mutation({
  args: {
    key: v.string(),
    bucket: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const metadata = await ctx.db
      .query("metadata")
      .withIndex("bucket_key", (q) =>
        q.eq("bucket", args.bucket).eq("key", args.key),
      )
      .unique();
    if (metadata) {
      await ctx.db.delete("metadata", metadata._id);
    }
  },
});

const httpStatusCode = (error: unknown) =>
  (error as { $metadata?: { httpStatusCode?: number } }).$metadata
    ?.httpStatusCode;

const normalizeEtag = (etag: string) =>
  etag.replace(/^W\//, "").replace(/^"|"$/g, "");

// Copy an object to another key on the server side. Streaming a GET body into a
// signed PUT sends it chunked without Content-Length, and R2 refuses that with
// 411, so large objects can only move with a CopyObject request.
export const copyR2Object = action({
  args: {
    sourceKey: v.string(),
    destinationKey: v.string(),
    expectedSize: v.optional(v.number()),
    expectedEtag: v.optional(v.string()),
    ...r2ConfigValidator.fields,
  },
  returns: v.union(
    v.object({
      outcome: v.literal("copied"),
      size: v.optional(v.number()),
      etag: v.optional(v.string()),
    }),
    v.object({ outcome: v.literal("source_missing") }),
    v.object({ outcome: v.literal("source_changed") }),
  ),
  handler: async (_ctx, args) => {
    const { sourceKey, destinationKey, expectedSize, expectedEtag, ...r2Config } =
      args;
    const r2 = createR2Client(r2Config);

    let source;
    try {
      source = await r2.send(
        new HeadObjectCommand({ Bucket: r2Config.bucket, Key: sourceKey }),
      );
    } catch (error) {
      if (httpStatusCode(error) === 404) {
        return { outcome: "source_missing" as const };
      }
      throw error;
    }
    if (
      (expectedSize !== undefined && source.ContentLength !== expectedSize) ||
      (expectedEtag !== undefined &&
        (source.ETag === undefined ||
          normalizeEtag(source.ETag) !== normalizeEtag(expectedEtag)))
    ) {
      return { outcome: "source_changed" as const };
    }

    try {
      await r2.send(
        new CopyObjectCommand({
          Bucket: r2Config.bucket,
          Key: destinationKey,
          CopySource: `${r2Config.bucket}/${sourceKey
            .split("/")
            .map(encodeURIComponent)
            .join("/")}`,
          // Copy exactly the object the head above described; R2 answers 412
          // when it changed in between.
          CopySourceIfMatch: source.ETag,
        }),
      );
    } catch (error) {
      const status = httpStatusCode(error);
      if (status === 404) {
        return { outcome: "source_missing" as const };
      }
      if (status === 412) {
        return { outcome: "source_changed" as const };
      }
      throw error;
    }

    return {
      outcome: "copied" as const,
      size: source.ContentLength,
      etag: source.ETag,
    };
  },
});

export const deleteR2Object = action({
  args: {
    key: v.string(),
    ...r2ConfigValidator.fields,
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const { key, ...r2Config } = args;
    const r2 = createR2Client(r2Config);
    await r2.send(
      new DeleteObjectCommand({ Bucket: r2Config.bucket, Key: key }),
    );
  },
});

export const deleteObject = mutation({
  args: {
    key: v.string(),
    ...r2ConfigValidator.fields,
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const metadata = await ctx.db
      .query("metadata")
      .withIndex("bucket_key", (q) =>
        q.eq("bucket", args.bucket).eq("key", args.key),
      )
      .unique();
    if (metadata) {
      await ctx.db.delete("metadata", metadata._id);
    }
    await retrier.run(ctx, api.lib.deleteR2Object, args);
  },
});

export const withoutSystemFields = <T extends Doc<TableNames>>(fields: T) => {
  const { _id, _creationTime, ...rest } = fields;
  return rest;
};
