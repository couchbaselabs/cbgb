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

function BucketStatsCtrl($scope, $routeParams, $http) {
  var bucketName = $routeParams.bucketName
  $http.get('/api/buckets/' + bucketName + '/stats').
    success(function(data) {
      $scope.bucketName = bucketName;
      $scope.bucketStats = data;
      $scope.err = null;
    }).
    error(function() {
      $scope.err = restErrorMsg
    });
}
