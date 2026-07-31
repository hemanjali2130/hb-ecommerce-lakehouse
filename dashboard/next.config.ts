import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,

  // Fail the build on type errors rather than shipping them. Worth stating
  // explicitly: the whole "no AWS SDK in client components" guarantee rests on
  // `import "server-only"` throwing at BUILD time, which only helps if the build
  // is actually allowed to fail.
  typescript: { ignoreBuildErrors: false },
  eslint: { ignoreDuringBuilds: true },
};

export default nextConfig;
