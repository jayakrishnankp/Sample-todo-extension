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
        let removeButton = document.createElement('button');
        removeButton.textContent = 'x';
        removeButton.id = element.id.toString();
        removeButton.classList.add('removeButton');
        removeButton.addEventListener('click', removeTask);
        node.appendChild(removeButton);
        taskList.appendChild(node);
      });
    }
  });
}

document.getElementById("taskForm").addEventListener("submit", submitForm);

function submitForm(e){
  e.preventDefault();
  let title = document.querySelector('#inputTitle').value;
  let content = document.querySelector('#inputContent').value;

  window.taskStore.insert({title, content});
  showData();
}

function removeTask(e){
  window.taskStore.delete({id: e.target.id});
  showData();
}

setTimeout(showData, 1000);