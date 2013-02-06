function ServerCtrl($scope, $http) {
  $http.get('/api/serverSettings').success(function(data) {
      $scope.serverSettings = data;
      $scope.serverSettingsKeys = _.keys(data)
  });
  $http.get('/api/bucketSettings').success(function(data) {
      $scope.bucketSettings = data;
      $scope.bucketSettingsKeys = _.keys(data)
  });
}
