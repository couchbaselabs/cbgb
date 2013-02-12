angular.module('cbgb', []).
  config(['$routeProvider', function($routeProvider) {
  $routeProvider.
      when('/server',
           {templateUrl: 'partials/server.html',
            controller: ServerCtrl}).
      when('/buckets',
           {templateUrl: 'partials/bucket-list.html',
            controller: BucketListCtrl}).
      when('/buckets/:bucketName',
           {templateUrl: 'partials/bucket-detail.html',
            controller: BucketDetailCtrl}).
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

function BucketListCtrl($scope, $http) {
  $http.get('/api/buckets').
    success(function(data) {
      $scope.names = data;
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}

function BucketDetailCtrl($scope, $routeParams, $http) {
  $http.get('/api/buckets/' + $routeParams.bucketName).
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

function BucketStatsCtrl($scope, $routeParams, $http, $timeout) {
  var drawChart = makeChart("ops", 10, 100);
  var bucketName = $routeParams.bucketName;

  function go() {
    $http.get('/api/buckets/' + bucketName + '/stats').
      success(function(data) {
        $scope.bucketName = bucketName;
        $scope.bucketStats = data;
        $scope.err = null;
        drawChart(data.diffs.bucketStats.levels[0]);
        $timeout(go, 1000)
      }).
      error(function() {
        $scope.err = restErrorMsg
        $timeout(go, 1000)
      });
  }

  go()
}

function makeChart(field, barW, barH) {
  return function(data) {
    var yMin = 0xffffffff;
    var yMax = 0;
    for (var i = 0; i < data.length; i++) {
      if (yMin > data[i][field]) {
        yMin = data[i][field];
      }
      if (yMax < data[i][field]) {
        yMax = data[i][field];
      }
    }
    if (yMax - yMin < barH) {
      yMax = yMin + barH;
    }

    var xScale = d3.scale.linear()
      .domain([0, 1])
      .range([0, barW]);

    var yScale = d3.scale.linear()
      .domain([yMin, yMax])
      .rangeRound([0, barH]);

    var chart = d3.select("#chart")
      .attr("width", barW * data.length - 1)
      .attr("height", barH);

    var rect = chart.selectAll("rect")
      .data(data, function(d) { return d.time; })

    var duration = 1000;
    var xMargin = 0.5;
    var yMargin = 0.5;
    var yMin = 2;

    rect.enter()
      .insert("rect")
      .attr("x", function(d, i) { return xScale(i + 1) - xMargin; })
      .attr("y", function(d) {
          return barH - (yMin + yScale(d[field])) - yMargin;
        })
      .attr("width", barW)
      .attr("height", function(d) { return yMin + yScale(d[field]); })
      .transition()
      .duration(duration)
      .attr("x", function(d, i) { return xScale(i) - xMargin; });

    rect.transition()
      .duration(duration)
      .attr("x", function(d, i) { return xScale(i) - xMargin; })
      .attr("y", function(d) {
          return barH - (yMin + yScale(d[field])) - yMargin;
        })
      .attr("height", function(d) { return yMin + yScale(d[field]); });

    rect.exit()
      .transition()
      .duration(duration)
      .attr("x", function(d, i) { return -barW - xMargin; })
      .remove();
  }
}
