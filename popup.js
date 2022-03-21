function showData(){
  let taskList = document.querySelector('#taskList');
  taskList.innerHTML = '';
  window.taskStore.get(function(tasks){
    if(tasks){
      rows = Object.values(tasks);
      rows.forEach(element => {
        let node = document.createElement('li');
        node.id = element.id;
        node.classList.add('listItem');
        node.innerText = element.title;
        taskList.appendChild(node);
      });
    }
  });
}

setTimeout(showData, 200);