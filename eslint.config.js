import globals from 'globals';

const sharedRules = {
  'no-unused-vars': 'warn',
  'no-undef': 'error',
};

const sharedLanguageOptions = {
  ecmaVersion: 'latest',
  sourceType: 'module',
};

export default [
  {
    files: ['src/backend/**/*.js', 'scripts/**/*.js', 'worker.js', 'vite.config.js', 'eslint.config.js'],
    languageOptions: {
      ...sharedLanguageOptions,
      globals: {
        ...globals.node,
      },
    },
    rules: sharedRules,
  },
  {
    files: ['src/frontend/**/*.{js,jsx}'],
    languageOptions: {
      ...sharedLanguageOptions,
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
      globals: {
        ...globals.browser,
      },
    },
    rules: sharedRules,
  },
];
