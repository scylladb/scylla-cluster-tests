module.exports = {
    rules: {
        // Subject
        'subject-empty': [2, 'never'],
        'subject-full-stop': [2, 'never', '.'],
        'subject-max-length': [2, 'always', 85],
        'subject-min-length': [2, 'always', 10],
        // Type
        'type-enum': [2,'always',['ci','docs','feature','fix','improvement','perf','refactor','revert','style','test', 'unit-test']],
        'type-empty': [2, 'never'],
        // Scope
        'scope-empty': [1, 'never'],
        'scope-case': [2, 'always', ['lower-case']],
        'scope-min-length': [2, 'always', 5],
        // Body
        'body-min-length': [2, 'always', 30],
        'body-max-line-length': [2, 'always', 100],
        'body-leading-blank': [2, 'always'],
    }
};
