angular.module('cbgb', []).
  config(['$routeProvider', function($routeProvider) {
  $routeProvider.
      when('/server',
           {templateUrl: 'partials/server.html',
            controller: ServerCtrl}).
      when('/buckets',
           {templateUrl: 'partials/buckets.html',
            controller: BucketsCtrl}).
      when('/buckets/:bucketName',
           {templateUrl: 'partials/bucket.html',
            controller: BucketCtrl}).
      when('/buckets/:bucketName/stats',
           {templateUrl: 'partials/bucket-stats.html',
            controller: BucketStatsCtrl}).
      otherwise({redirectTo: '/server'});
}]);

var restErrorMsg = "error communicating with server; please try again.";

function ServerCtrl($scope, $http) {
  $http.get('/api/settings').
    success(function(data) {
      $scope.settings = data;
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}

function BucketsCtrl($scope, $http) {
  $scope.bucketName = "";
  $scope.bucketNamePattern = /^[A-Za-z0-9\-_]+$/;

  $scope.bucketCreate = function() {
    var bucketName = $scope.bucketName;
    if (bucketName.length <= 0) {
      return
    }

    if (!bucketName.match($scope.bucketNamePattern)) {
      $scope.bucketCreateResult =
        "error: please use alphanumerics, dashes, and underscores only";
      return
    }

    if (_.contains($scope.names, bucketName)) {
      $scope.bucketCreateResult =
      "error: bucket " + bucketName + " already exists";
      return
    }

    $scope.bucketCreateResult = "creating bucket: " + bucketName + " ...";

    $http({
        method: 'POST',
        url: '/api/buckets',
        data: 'bucketName=' + encodeURIComponent(bucketName),
        headers: {'Content-Type': 'application/x-www-form-urlencoded'}
      }).
      success(function(data) {
        console.log(data);
        $scope.bucketCreateResult =
          "created bucket: " + bucketName;
        retrieveBucketNames();
      }).
      error(function(data) {
        $scope.bucketCreateResult =
          "error creating bucket: " + bucketName + "; error: " + data;
      });
  }

  function retrieveBucketNames() {
    $http.get('/api/buckets').
      success(function(data) {
        $scope.names = data.sort();
        $scope.err = null;
      }).
      error(function() {
        $scope.err = restErrorMsg
      });
  }

  retrieveBucketNames();
}

function BucketCtrl($scope, $routeParams, $http, $location) {
  $scope.bucketName = $routeParams.bucketName;

  $scope.flushBucketDirtyItems = function() {
    $http.post("/api/buckets/" + $scope.bucketName + "/flushDirty").
      success(function() {
          alert("Dirty items for bucket '" + $scope.bucketName +
                "' were flushed to disk.");
      }).
      error(function(data) {
          alert("Dirty items for bucket '" + $scope.bucketName +
                "' were not flushed to disk; error: " + data);
      });
  }

  $scope.compactBucket = function() {
    $http.post("/api/buckets/" + $scope.bucketName + "/compact").
      success(function() {
          alert("Bucket '" + $scope.bucketName +
                "' was compacted.");
      }).
      error(function(data) {
          alert("Bucket '" + $scope.bucketName +
                "' was not compacted; error: " + data);
      });
  }

  $scope.deleteBucket = function() {
    if (confirm("Are you sure you want to permanently delete bucket '" +
                $scope.bucketName +
                "', including erasing all its data items?")) {
      $http.delete("/api/buckets/" + $scope.bucketName).
        success(function() {
            $location.path("/buckets");
            $scope.$apply();
        }).
        error(function(data) {
            alert("Bucket '" + $scope.bucketName +
                  "' was not deleted; error: " + data);
        });
    }
  }

  $http.get('/api/buckets/' + $scope.bucketName).
    success(function(data) {
      $scope.bucket = data;
      $scope.bucket.partitionsArray = _.values(data.partitions);
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });

  $scope.orderChoice = 'id';
}

var lastChartId = (new Date()).getTime();

function BucketStatsCtrl($scope, $routeParams, $http, $timeout) {
  $scope.bucketName = $routeParams.bucketName;
  $scope.currLevel = 0;
  $scope.currStatKind = "bucketStats";
  $scope.currStatName = "ops";
  $scope.paused = false;

  $scope.stopChart = function() {
    if ($scope.drawChart) {
      $scope.drawChart(null);
      $scope.drawChart = null;
    }
    if ($scope.timeout) {
      $timeout.cancel($scope.timeout);
      $scope.timeout = null;
    }
  }

  $scope.changeLevel = function(i) {
    $scope.stopChart();
    $scope.currLevel = i;
    $scope.paused = false;
    go();
  }

  $scope.changeStat = function(kind, statName) {
    $scope.stopChart();
    $scope.currStatKind = kind;
    $scope.currStatName = statName;
    $scope.paused = false;
    go();
  }

  function go() {
    if ($scope.paused) {
        $scope.timeout = $timeout(go, 1000);
        return;
    }

    $http.get('/api/buckets/' + $scope.bucketName + '/stats').
      success(function(data) {
        $scope.err = null;
        $scope.stats = data;
        $scope.statNames =
          _.union(_.map(_.without(_.keys(data.totals.bucketStats), "time"),
                        function(x) { return ["bucketStats", x]; }),
                  _.map(_.without(_.keys(data.totals.bucketStoreStats), "time"),
                        function(x) { return ["bucketStoreStats", x]; }))

        if (!$scope.drawChart) {
          lastChartId++;
          $scope.drawChart =
            makeChart(lastChartId,
                      $scope.currStatName,
                      data.levels[$scope.currLevel].numSamples,
                      10, 400);
        }
        $scope.drawChart(data.diffs[$scope.currStatKind].levels[$scope.currLevel]);
        $scope.timeout = $timeout(go, 1000);
      }).
      error(function() {
        $scope.err = restErrorMsg;
        $scope.timeout = $timeout(go, 1000);
      });
  }

  go();
}

function makeChart(chartId, statName, dataLength, barW, barH) {
  var duration = 400;
  var xMargin = 0.5;
  var yMargin = 0.5;
  var done = false;

  var xScale = d3.scale.linear()
    .domain([0, 1])
    .range([0, barW]);

  return function(data) {
    if (!data || lastChartId != chartId) {
      done = true;
      d3.select("#chart" + chartId)
        .selectAll("rect")
        .data([])
        .exit()
        .remove();
      d3.select("#chart" + chartId)
        .remove();
    }
    if (done) {
      return;
    }

    function idx(i) {
      // Handles when data length is too small.
      if (dataLength > data.length) {
        return i + dataLength - data.length;
      }
      return i;
    }

    var yMin = 0xffffffff;
    var yMax = 0;
    for (var i = 0; i < data.length; i++) {
      if (yMin > data[i][statName]) {
        yMin = data[i][statName];
      }
      if (yMax < data[i][statName]) {
        yMax = data[i][statName];
      }
    }
    if (yMax - yMin < barH) {
      yMax = yMin + barH;
    }

    var yScale = d3.scale.linear()
      .domain([yMin, yMax])
      .rangeRound([2, barH]);

    if (!document.getElementById("chart" + chartId)) {
      d3.select("#charts")
        .append("svg:svg")
        .attr("id", "chart" + chartId)
        .attr("class", "chart")
        .attr("width", barW * dataLength - 1)
        .attr("height", barH);
    }

    var chart = d3.select("#chart" + chartId);

    var rect = chart.selectAll("rect")
      .data(data, function(d) { return d.time; })

    rect.enter()
      .insert("rect")
      .attr("x", function(d, i) { return xScale(idx(i + 1)) - xMargin; })
      .attr("y", function(d) {
          return barH - yScale(d[statName]) - yMargin;
        })
      .attr("width", barW)
      .attr("height", function(d) { return yMin + yScale(d[statName]); })
      .transition().duration(duration)
      .attr("x", function(d, i) { return xScale(idx(i)) - xMargin; });

    rect.transition().duration(duration)
      .attr("x", function(d, i) { return xScale(idx(i)) - xMargin; })
      .attr("y", function(d) {
          return barH - yScale(d[statName]) - yMargin;
        })
      .attr("height", function(d) { return yMin + yScale(d[statName]); });

    rect.exit()
      .transition().duration(duration)
      .attr("x", function(d, i) { return -barW - xMargin; })
      .remove();
  }
}
