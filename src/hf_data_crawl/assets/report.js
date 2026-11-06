
(function(){
  function cells(row){return row.children;}
  document.querySelectorAll('input[data-filter]').forEach(function(box){
    var table=document.getElementById(box.dataset.filter);
    if(!table) return;
    var count=document.getElementById(box.dataset.filter+'-count');
    var rows=Array.prototype.slice.call(table.tBodies[0].rows);
    var timer=null;
    function apply(){
      var q=box.value.trim().toLowerCase();
      var shown=0;
      rows.forEach(function(row){
        var hit=!q || row.textContent.toLowerCase().indexOf(q)>-1;
        row.hidden=!hit;
        if(hit) shown++;
      });
      if(count) count.textContent=shown+' / '+rows.length;
    }
    box.addEventListener('input',function(){clearTimeout(timer);timer=setTimeout(apply,120);});
    apply();
  });

  document.querySelectorAll('.seg button').forEach(function(btn){
    btn.addEventListener('click',function(){
      var table=document.getElementById(btn.dataset.target);
      if(!table) return;
      var key=btn.dataset.key;
      var body=table.tBodies[0];
      Array.prototype.slice.call(body.rows)
        .sort(function(a,b){return (parseFloat(b.dataset['k'+key])||0)-(parseFloat(a.dataset['k'+key])||0);})
        .forEach(function(row){body.appendChild(row);});
      btn.parentNode.querySelectorAll('button').forEach(function(other){
        other.setAttribute('aria-pressed', other===btn ? 'true':'false');
      });
    });
  });
})();
