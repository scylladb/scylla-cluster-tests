module.exports = {
    rules: {
        // Header
        "header-max-length": [2, "always", 100],
        // Subject
        'subject-empty': [2, 'never'],
        'subject-full-stop': [2, 'never', '.'],
        'subject-max-length': [2, 'always', 120],
        'subject-min-length': [2, 'always', 10],
        // Type
        'type-enum': [2,'always',['ci','docs','feature','fix','improvement','perf','refactor','revert','style','test', 'unit-test', 'build', 'chore']],
        'type-empty': [2, 'never'],
        // Scope
        'scope-empty': [2, 'never'],
        'scope-min-length': [2, 'always', 3],
        // Body
        'body-min-length': [2, 'always', 30],
        'body-max-line-length': [2, 'always', 120],
        'body-leading-blank': [2, 'always'],
    },
    parserPreset: {
      parserOpts: {
        noteKeywords: ['\\[\\d+\\]']
      }
    }
};
