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
  var drawChart = makeChart("ops", 60, 10, 100);
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

function makeChart(field, dataLength, barW, barH) {
  var x = d3.scale.linear()
    .domain([0, 1])
    .range([0, barW]);

  var y = d3.scale.linear()
    .domain([0, 100])
    .rangeRound([0, barH]);

  return function(data) {
    var chart = d3.select("#chart")
      .attr("width", barW * dataLength - 1)
      .attr("height", barH);

    var rect = chart.selectAll("rect")
      .data(data, function(d) { return d.time; })

    rect.enter()
        .insert("rect")
          .attr("x", function(d, i) { return x(i + 1) - .5; })
          .attr("y", function(d) { return barH - (5 + y(d[field])) - .5; })
          .attr("width", barW)
          .attr("height", function(d) { return 5 + y(d[field]); })
        .transition()
          .duration(1000)
          .attr("x", function(d, i) { return x(i) - .5; });

    rect.transition()
        .duration(1000)
        .attr("x", function(d, i) { return x(i) - .5; });

    rect.exit()
        .transition()
          .duration(1000)
          .attr("x", function(d, i) { return x(i - 1) - .5; })
          .remove();
  }
}
