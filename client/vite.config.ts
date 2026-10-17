import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    fs: { allow: ['..'] },
    proxy: {
      '/ws': { target: 'ws://127.0.0.1:8787', ws: true },
      '/api': { target: 'http://127.0.0.1:8787' },
    },
  },
  resolve: {
    alias: {
      '@shared': path.resolve(__dirname, '../shared'),
    },
  },
});
