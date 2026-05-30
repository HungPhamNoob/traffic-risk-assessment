/** @type {import('next').NextConfig} */
const nextConfig = {
  transpilePackages: [
    "@deck.gl/layers",
    "@deck.gl/react",
    "maplibre-gl",
    "react-map-gl"
  ]
};

export default nextConfig;
