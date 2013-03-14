(function() {

  var progressWrapper = $('#global_progress');
  var progressContainer = $('#global_progress_container');

  var render = {};

  function renderNothing() {
    return "";
  }

  function refreshProgress(tasks) {
    var html = "";
    _.each(tasks, function (obj) {
      html += (render[obj.type] || renderNothing)(obj);
    });
    if (html == "") {
      progressWrapper.hide('fade');
    } else {
      progressWrapper.toggleClass('disable_toggle', tasks.length < 2);
      progressContainer.html(html);
      progressWrapper.show();
    }
  }

  render.indexer = function (obj) {
    return '<li class="clearfix"><div class="usage_smallest">' +
      '<div class="used" style="width:' + (obj.progress >> 0) +
      '%"></div></div><span class="message">Indexing ' + escapeHTML(obj.bucket + "/" + obj.designDocument) + '</span></li>';
  };

  render.view_compaction = function (obj) {
    return '<li class="clearfix"><div class="usage_smallest">' +
      '<div class="used" style="width:' + (obj.progress >> 0) +
      '%"></div></div><span class="message">Compacting index ' + escapeHTML(obj.bucket + "/" + obj.designDocument) + '</span></li>';
  };

  render.bucket_compaction = function (obj) {
    return '<li class="clearfix"><div class="usage_smallest">' +
      '<div class="used" style="width:' + (obj.progress >> 0) +
      '%"></div></div><span class="message">Compacting bucket ' +
      escapeHTML(obj.bucket) + '</span></li>';
  };

  render.rebalance = function (obj) {
    if (obj.status !== "running") {
      return "";
    }

    var serversCount = _.keys((obj.perNode || {})).length;

    return '<li class="clearfix"><div class="usage_smallest">' +
      '<div class="used" style="width:' + (obj.progress >> 0) +
      '%"></div></div><span class="message">Rebalancing ' + serversCount +
      ' nodes</span></li>';
  };

  render.loadingSampleBucket = function (obj) {
    return '<li class="clearfix"><span class="message">Loading sample: ' + escapeHTML(obj.bucket) + '&nbsp;</span></li>';
  }


  progressWrapper.find('.toggle').bind('click', function() {
    if (!progressWrapper.is('.disable_toggle')) {
      progressWrapper.toggleClass('closed');
    }
  });

  var runningTasks = Cell.compute(function (v) {
    return _.filter(v.need(DAL.cells.tasksProgressCell), function (taskInfo) {
      return taskInfo.status === "running";
    })
  });

  runningTasks.subscribeValue(function (tasks) {
    refreshProgress(tasks);
  });

})();
