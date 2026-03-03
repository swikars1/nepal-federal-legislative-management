import { NextRequest, NextResponse } from "next/server";

type RateLimitOptions = {
  /** Sliding window length in milliseconds */
  windowMs: number;
  /** Max requests allowed within the window */
  max: number;
};

type Bucket = {
  count: number;
  resetAt: number;
};

// Use a global map so the limiter survives hot reloads in dev
const buckets: Map<string, Bucket> =
  (globalThis as unknown as { __rateLimitBuckets?: Map<string, Bucket> })
    .__rateLimitBuckets || new Map();

(
  globalThis as unknown as { __rateLimitBuckets?: Map<string, Bucket> }
).__rateLimitBuckets = buckets;

const DEFAULT_OPTS: RateLimitOptions = { windowMs: 60_000, max: 60 }; // 60 req/min per identifier

export function getClientIdentifier(request: NextRequest): string {
  const forwarded = request.headers.get("x-forwarded-for");
  const ip =
    forwarded?.split(",")[0]?.trim() ||
    (request as unknown as { ip?: string }).ip;
  return ip || "unknown";
}

export function rateLimit(
  identifier: string,
  options: RateLimitOptions = DEFAULT_OPTS,
): { limited: boolean; headers: Headers } {
  const now = Date.now();
  const bucket = buckets.get(identifier);

  if (!bucket || now > bucket.resetAt) {
    buckets.set(identifier, {
      count: 1,
      resetAt: now + options.windowMs,
    });
  } else {
    bucket.count += 1;
  }

  const current = buckets.get(identifier)!;
  const headers = new Headers();
  headers.set("X-RateLimit-Limit", options.max.toString());
  headers.set(
    "X-RateLimit-Remaining",
    Math.max(options.max - current.count, 0).toString(),
  );
  headers.set("X-RateLimit-Reset", current.resetAt.toString());

  const limited = current.count > options.max;
  if (limited) {
    const retryAfterSeconds = Math.max(
      0,
      Math.ceil((current.resetAt - now) / 1000),
    ).toString();
    headers.set("Retry-After", retryAfterSeconds);
  }

  return { limited, headers };
}

export function rateLimitOrJson(
  request: NextRequest,
  options?: RateLimitOptions,
): NextResponse | null {
  const id = getClientIdentifier(request);
  const { limited, headers } = rateLimit(id, options);
  if (!limited) return null;

  return NextResponse.json(
    { error: "Too many requests" },
    {
      status: 429,
      headers,
    },
  );
}
