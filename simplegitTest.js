const simpleGit = require('simple-git');
const git = simpleGit();

git.init().addRemote('origin', 'https://github.com/subutuo/CCNB_PCK_LOG.git');


git.raw('rev-list', '--all', '--count').then((i) => {
  console.log(i)
})

//git.push('origin', 'master');



// git
//   .add('.')
//   .commit("first commit!");


// git.log((err, log) => {
//   console.log(log);
// });



//.add([fileA, ...], handlerFn)
//.commit(message, handlerFn)

 
function onInit (err, initResult) { }
/*

require('simple-git')()
    .addConfig('user.name', 'Some One')
    .addConfig('user.email', 'some@one.com')
    .commit('committed as "Some One"', 'file-one')
    .commit('committed as "Another Person"', 'file-two', { '--author': '"Another Person <another@person.com>"' });

require('simple-git')()
     .init()
     .add('./*')
     .commit("first commit!")
     .addRemote('origin', 'https://github.com/user/repo.git')
     .push('origin', 'master');
    */