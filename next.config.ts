import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  // instrumentation.ts автостартует ядро-сканер (pump-service, порт 3003) вместе с сервером
  typescript: {
    ignoreBuildErrors: true,
  },
  reactStrictMode: false,
};

export default nextConfig;
