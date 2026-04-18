import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // In dev, /api/* → http://localhost:8000/api/* (no CORS, no absolute URLs needed)
      // Set VITE_API_BASE_URL=/api/v1 in .env to activate this path.
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
