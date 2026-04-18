/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: { sans: ['Inter', 'system-ui', 'sans-serif'] },
      colors: {
        rose:   '#FF6B8A',
        amber:  '#FFB347',
        mint:   '#56D19E',
        violet: '#A78BFA',
        indigo: '#C084FC',
        brand:  '#F9A8C0',   // Avalon warm rose brand
      },
      borderRadius: {
        glass: '15px',
      },
      transitionTimingFunction: {
        avalon: 'cubic-bezier(0.4, 0, 0.2, 1)',
      },
    },
  },
  plugins: [],
}
