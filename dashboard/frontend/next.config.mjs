/** @type {import('next').NextConfig} */
const nextConfig = {
  transpilePackages: [
    "@deck.gl/layers",
    "@deck.gl/react",
    "maplibre-gl",
    "react-map-gl"
  ],
  async rewrites() {
    const target = process.env.DASHBOARD_PROXY_TARGET || "http://localhost:8000";
    return [
      {
        source: "/api-proxy/:path*",
        destination: `${target}/:path*`
      }
    ];
  }
};

export default nextConfig;
