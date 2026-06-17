import { defineConfig } from 'vite'
import { resolve } from 'path'

export default defineConfig({
  build: {
    outDir: resolve(__dirname, 'static'),
    emptyOutDir: true, // Clean the static output folder before build
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
