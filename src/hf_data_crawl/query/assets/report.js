/* 榜单页交互 · 无依赖、无网络。
   五件事：全局筛选、表头排序、下载榜双榜切换、分布度量切换、明暗彩蛋。
   所有查询都判空：这些节点由渲染器生成，少一个只影响那一项能力，
   不该让整页脚本停摆——缺节点时跳过对应逻辑并保留其余交互。 */

(function () {
  'use strict';

  var q = document.getElementById('q');
  var status = document.getElementById('q-status');
  var measure = 0;
  var timer = null;

  function visible(table) {
    return !table.hidden && !table.closest('[hidden]');
  }

  function dataRows(table) {
    var body = table.tBodies[0];
    if (!body) return [];
    return Array.prototype.slice.call(body.rows).filter(function (row) {
      return !row.hasAttribute('data-empty') && !row.hasAttribute('data-no-hit');
    });
  }

  function clearNoHit(table) {
    var body = table.tBodies[0];
    if (!body) return;
    Array.prototype.slice.call(body.querySelectorAll('tr[data-no-hit]'))
      .forEach(function (row) { row.remove(); });
  }

  function noHitRow(table, label) {
    var row = document.createElement('tr');
    row.setAttribute('data-no-hit', '');
    row.className = 'no-hit';
    var cell = document.createElement('td');
    cell.colSpan = Number(table.dataset.cols) || 1;
    cell.textContent = '没有匹配「' + label + '」的行。试试数据集 id、正式名、标签或许可证。';
    row.appendChild(cell);
    return row;
  }

  function writeCount(board, shown, total) {
    var badge = board.querySelector('.board-count');
    // 空态 board 里可能只有一段说明文字，没有表；此时不留一个刺眼的 0 / 0。
    if (badge) badge.textContent = total ? shown + ' / ' + total : '';
  }

  function applyFilter() {
    var label = q ? q.value.trim() : '';
    var needle = label.toLowerCase();
    var shownAll = 0;
    var totalAll = 0;

    document.querySelectorAll('.board').forEach(function (board) {
      var tables = Array.prototype.slice.call(board.querySelectorAll('table.data[data-searchable]'));
      var active = tables.filter(visible);
      var shown = 0;
      var total = 0;

      active.forEach(function (table) {
        clearNoHit(table);
        var body = table.tBodies[0];
        if (!body) return;
        var rows = dataRows(table);
        var local = 0;
        total += rows.length;
        rows.forEach(function (row) {
          var hit = !needle || (row.dataset.search || '').indexOf(needle) > -1;
          row.hidden = !hit;
          if (hit) local++;
        });
        shown += local;
        if (local === 0 && needle) body.appendChild(noHitRow(table, label));
      });

      writeCount(board, shown, total);
      shownAll += shown;
      totalAll += total;
    });

    if (status) {
      status.textContent = needle
        ? '命中 ' + shownAll + ' / ' + totalAll + ' 行'
        : '共 ' + totalAll + ' 行';
    }
  }

  function sortValue(row, colIndex) {
    var cell = row.cells[colIndex];
    if (!cell) return '';
    if (cell.classList.contains('share')) {
      return Number(cell.dataset['v' + measure]) || 0;
    }
    if (cell.dataset.n) {
      var number = Number(cell.dataset.n);
      if (!isNaN(number)) return number;
    }
    return cell.textContent.trim();
  }

  function sortTable(table, colIndex, direction) {
    var body = table.tBodies[0];
    if (!body) return;
    var factor = direction === 'ascending' ? 1 : -1;
    dataRows(table).sort(function (a, b) {
      var x = sortValue(a, colIndex);
      var y = sortValue(b, colIndex);
      if (typeof x === 'number' && typeof y === 'number') return (x - y) * factor;
      return String(x).localeCompare(String(y), 'zh-CN') * factor;
    }).forEach(function (row) { body.appendChild(row); });
  }

  function onSort(th) {
    var table = th.closest('table');
    if (!table) return;
    var next = th.getAttribute('aria-sort') === 'ascending' ? 'descending' : 'ascending';
    table.querySelectorAll('th[aria-sort]').forEach(function (other) {
      other.setAttribute('aria-sort', 'none');
    });
    th.setAttribute('aria-sort', next);
    sortTable(table, Number(th.dataset.col), next);
    applyFilter();
  }

  document.querySelectorAll('th[data-sort]').forEach(function (th) {
    th.setAttribute('tabindex', '0');
    th.setAttribute('title', '点击或按回车排序（仅当前榜内）');
    th.addEventListener('click', function () { onSort(th); });
    th.addEventListener('keydown', function (event) {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        onSort(th);
      }
    });
  });

  document.querySelectorAll('.seg button[data-target]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var group = btn.dataset.group;
      document.querySelectorAll('.seg button[data-group="' + group + '"]').forEach(function (peer) {
        peer.setAttribute('aria-pressed', peer === btn ? 'true' : 'false');
        var table = document.getElementById(peer.dataset.target);
        if (table) table.hidden = peer !== btn;
      });
      applyFilter();
    });
  });

  document.querySelectorAll('.seg button[data-measure]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      measure = Number(btn.dataset.measure) || 0;
      document.querySelectorAll('.seg button[data-measure]').forEach(function (peer) {
        peer.setAttribute('aria-pressed', peer === btn ? 'true' : 'false');
      });

      document.querySelectorAll('table[data-measurable]').forEach(function (table) {
        var caption = table.querySelector('caption');
        if (caption) caption.textContent = table.dataset['cap' + measure] || table.dataset.cap0 || '';

        var head = table.querySelector('th.share-col');
        if (head) head.textContent = head.dataset['label' + measure] || head.textContent;

        Array.prototype.slice.call(table.tBodies[0].rows).forEach(function (row) {
          var cell = row.querySelector('td.share');
          if (!cell) return;
          var pct = Number(cell.dataset['p' + measure]) || 0;
          cell.style.setProperty('--w', pct.toFixed(2) + '%');
          var number = cell.querySelector('.share-num');
          if (number) number.textContent = pct.toFixed(1) + '%';
        });

        var sorted = table.querySelector('th.share-col[aria-sort="ascending"], th.share-col[aria-sort="descending"]');
        if (sorted) sortTable(table, Number(sorted.dataset.col), sorted.getAttribute('aria-sort'));
      });

      applyFilter();
    });
  });

  // 双击页边（避开链接、控件与表格）切换明暗；只改根节点的 data-theme，
  // 不做持久化——file:// 下 localStorage 常被禁用，彩蛋本就只需活到关页。
  document.addEventListener('dblclick', function (event) {
    var target = event.target;
    if (!(target instanceof Element)) return;
    if (target.closest('a,input,button,select,textarea,summary,table,details')) return;
    var root = document.documentElement;
    root.dataset.theme = root.dataset.theme === 'light' ? 'dark' : 'light';
  });

  document.addEventListener('keydown', function (event) {
    if (!q) return;
    var active = document.activeElement;
    var typing = active && active.matches('input,textarea,select,[contenteditable="true"]');
    if (event.key === '/' && !typing) {
      event.preventDefault();
      q.focus();
      q.select();
    } else if (event.key === 'Escape' && active === q) {
      q.value = '';
      applyFilter();
      q.blur();
    }
  });

  if (q) {
    q.addEventListener('input', function () {
      window.clearTimeout(timer);
      timer = window.setTimeout(applyFilter, 120);
    });
  }

  applyFilter();
})();
