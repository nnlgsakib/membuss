import { defineConfig } from 'vite'
import { resolve } from 'path'

export default defineConfig({
  build: {
    outDir: resolve(__dirname, '../assets'),
    emptyOutDir: false, // Keep the Go templates (.html) in assets/
    minify: true,
    cssCodeSplit: false,
    rollupOptions: {
      input: {
        dag: resolve(__dirname, 'src/dag.js')
      },
      output: {
        entryFileNames: '[name].js',
        assetFileNames: '[name].[ext]'
      }
    }
  }
})
