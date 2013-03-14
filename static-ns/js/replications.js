/**
   Copyright 2011 Couchbase, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 **/
var ReplicationsModel = {};

(function () {
  var model = ReplicationsModel;
  var remoteClustersListURICell = model.remoteClustersListURICell = Cell.compute(function (v) {
    if (v.need(DAL.cells.mode) != 'replications')
      return;
    return v.need(DAL.cells.currentPoolDetailsCell).remoteClusters.uri;
  });
  var rawRemoteClustersListCell = model.remoteClustersAllListCell = Cell.compute(function (v) {
    return future.get({url: v.need(remoteClustersListURICell)}, function (list) {
      return _.sortBy(list, function (info) {return info.name});
    });
  });
  rawRemoteClustersListCell.keepValueDuringAsync = true;

  var remoteClustersListCell = model.remoteClustersListCell = Cell.compute(function (v) {
    return _.filter(v.need(rawRemoteClustersListCell), function (info) { return !info.deleted });
  });
  remoteClustersListCell.keepValueDuringAsync = true;

  var createReplicationURICell = model.createReplicationURICell = Cell.computeEager(function (v) {
    return v.need(DAL.cells.currentPoolDetailsCell).controllers.replication.createURI;
  });

  model.refreshReplications = function (softly) {
    if (!softly) {
      remoteClustersListCell.setValue(undefined);
    }
    rawRemoteClustersListCell.invalidate();
    DAL.cells.tasksProgressCell.invalidate();
  }

  var replicatorDBURIBaseCell = model.replicatorDBURIBaseCell = Cell.computeEager(function (v) {
    return v.need(DAL.cells.currentPoolDetailsCell).controllers.replication.replicatorDBURI + "/";
  });

})();

var ReplicationForm = mkClass({
  initialize: function () {
    this.dialog = $('#create_replication_dialog');
    this.form = this.dialog.find('form');
    this.onSubmit = $m(this, 'onSubmit');
    this.form.bind('submit', this.onSubmit);

    DAL.cells.bucketsListCell.subscribeValue(function(buckets) {
      if (!buckets) {
        return;
      }
      var rb = $('#replication_from_bucket');
      rb.html('<option value="">select a bucket</option>');
      _.each(buckets.byType.membase, function (bucket) {
        rb.append('<option>' + escapeHTML(bucket.name) + '</option>');
      });
    });
  },
  startCreate: function (callback) {
    var self = this;
    self.closeCallback = callback;
    ReplicationsModel.remoteClustersListCell.getValue(function (remoteClusters) {
      self.fillClustersSelect(remoteClusters);
      self.showErrors(false);
      setFormValues(self.form, {
        fromBucket: '',
        toBucket: '',
        toCluster: '',
        replicationType: 'continuous'
      });
      showDialog('create_replication_dialog');
    });
  },
  close: function () {
    hideDialog('create_replication_dialog');
    var callback = this.closeCallback;
    this.closeCallback = null;
    if (callback) {
      callback.apply(this, arguments);
    }
  },
  onSubmit: function (e) {
    e.preventDefault();

    var self = this;
    var URI = ReplicationsModel.createReplicationURICell.value;
    if (!URI) {
      return;
    }
    var spinner = overlayWithSpinner(self.dialog, null, "Creating replication...");
    var formValues = serializeForm(self.form);
    self.showErrors(false);
    jsonPostWithErrors(URI, formValues, function (data, status, errorObject) {
      spinner.remove();
      if (status == 'success') {
        self.close();
      } else {
        self.showErrors(errorObject || data);
      }
    });
  },
  fillClustersSelect: function (remoteClusters) {
    var toClusterSelect = $('#replication_to_cluster');
    toClusterSelect.html("<option value=''>Pick remote cluster</option>");
    _.map(remoteClusters, function (remoteCluster) {
      var option = $("<option></option>");
      option.text(remoteCluster.name);
      option.attr('value', remoteCluster.name);
      toClusterSelect.append(option);
    });
  },
  showErrors: function (errors) {
    var container = $('#create_replication_dialog_errors_container').need(1);
    if (!errors) {
      container.html('');
      return;
    }
    if ((errors instanceof Object) && errors.errors) {
      errors = _.values(errors.errors).sort();
    }
    renderTemplate('create_replication_dialog_errors', errors);
  }
});

// this turns ReplicationForm into lazily initialized singleton
mkClass.turnIntoLazySingleton(ReplicationForm);

ViewHelpers.formatReplicationStatus = function (info) {
  var rawStatus = escapeHTML(info.status);
  var errors = (info.errors || []);
  if (errors.length === 0) {
    return rawStatus;
  }

  var id = _.uniqueId('xdcr_errors');
  var rv = rawStatus + " <a onclick='showXDCRErrors(" + JSON.stringify(id) + ");'>Last " + ViewHelpers.count(errors.length, 'error');
  rv += "</a>"
  rv += "<script type='text/html' id='" + escapeHTML(id) + "'>"
  rv += JSON.stringify(errors)
  rv += "</script>"
  return rv;
}

function showXDCRErrors(id) {
  var text;
  try {
    text = document.getElementById(id).innerHTML;
  } catch (e) {
    console.log("apparently our data element is dead. Ignoring exception: ", e);
    return;
  }
  elements = JSON.parse(text);
  genericDialog({
    buttons: {ok: true},
    header: "XDCR errors",
    textHTML: "<ul>" + _.map(elements, function (anError) {return "<li>" + escapeHTML(anError) + "</li>"}).join('') + "</ul>"
  });
}

var ReplicationsSection = {
  init: function () {
    renderCellTemplate(ReplicationsModel.remoteClustersListCell, 'cluster_reference_list');

    var maybeXDCRTaskCell = Cell.compute(function (v) {
      var progresses = v.need(DAL.cells.tasksProgressCell);
      return _.filter(progresses, function (task) {
        return task.type === 'xdcr';
      });
    });
    maybeXDCRTaskCell.equality = _.isEqual;

    Cell.compute(function (v) {
      if (v.need(DAL.cells.mode) != 'replications') {
        return;
      }

      var replications = v.need(maybeXDCRTaskCell);
      var clusters = v.need(ReplicationsModel.remoteClustersListCell);
      var rawClusters = v.need(ReplicationsModel.remoteClustersAllListCell);

      return _.map(replications, function (replication) {
        var clusterUUID = replication.id.split("/")[0];
        var cluster = _.filter(clusters, function (cluster) {
          return cluster.uuid === clusterUUID;
        })[0];

        var name;
        if (cluster) {
          name = '"' + cluster.name + '"';
        } else {
          cluster = _.filter(rawClusters, function (cluster) {
            return cluster.uuid === clusterUUID;
          })[0];
          if (cluster) {
            // if we found cluster among rawClusters we assume it was
            // deleted
            name = 'at ' + cluster.hostname;
          } else {
            name = '"unknown"';
          }
        }

        return {
          id: replication.id,
          bucket: replication.source,
          to: 'bucket "' + replication.target.split('buckets/')[1] + '" on cluster ' + name,
          status: replication.status == 'running' ? 'Replicating' : 'Starting Up',
          when: replication.continuous ? "on change" : "one time sync",
          errors: replication.errors,
          cancelURI: replication.cancelURI
        }
      });
    }).subscribeValue(function (rows) {
      if (!rows) {
        return;
      }
      renderTemplate('ongoing_replications_list', rows);
    });

    $('#create_cluster_reference').click($m(this, 'startAddRemoteCluster'));
    $('#cluster_reference_list_container').delegate('.list_button.edit-button', 'click', function() {
      var name = $(this).closest('tr').attr('data-name');
      if (!name) {
        return;
      }
      ReplicationsSection.startEditRemoteCluster(name);
    }).delegate('.list_button.delete-button', 'click', function() {
      var name = $(this).closest('tr').attr('data-name');
      if (!name) {
        return;
      }
      ReplicationsSection.startDeleteRemoteCluster(name);
    });
    $('#create_replication').click($m(this, 'startCreateReplication'));
  },
  getRemoteCluster: function (name, body) {
    var self = this;
    var generation = {};
    self.getRemoteClusterGeneration = generation;
    ReplicationsModel.remoteClustersListCell.getValue(function (remoteClustersList) {
      if (generation !== self.getRemoteClusterGeneration) {
        return;
      }
      var remoteCluster = _.detect(remoteClustersList, function (candidate) {
        return (candidate.name === name)
      });
      body.call(self, remoteCluster);
    });
  },
  startEditRemoteCluster: function (name) {
    this.getRemoteCluster(name, function (remoteCluster) {
      if (!remoteCluster) {
        return;
      }
      editRemoteCluster(remoteCluster);
    });
    return;

    function editRemoteCluster(remoteCluster) {
      var form = $('#create_cluster_reference_dialog form');
      setFormValues(form, remoteCluster);
      $('#create_cluster_reference_dialog_errors_container').html('');
      showDialog('create_cluster_reference_dialog', {
        onHide: onHide
      });
      form.bind('submit', onSubmit);

      function onSubmit(e) {
        e.preventDefault();
        ReplicationsSection.submitRemoteCluster(remoteCluster.uri, form);
      }

      function onHide() {
        form.unbind('submit', onSubmit);
      }
    }
  },
  submitRemoteCluster: function (uri, form) {
    var spinner = overlayWithSpinner(form);
    jsonPostWithErrors(uri, form, function (data, status, errorObject) {
      spinner.remove();
      if (status == 'success') {
        hideDialog('create_cluster_reference_dialog');
        ReplicationsModel.refreshReplications();
        return;
      }
      renderTemplate('create_cluster_reference_dialog_errors', errorObject ? _.values(errorObject).sort() : data);
    })
  },
  startAddRemoteCluster: function () {
    var form = $('#create_cluster_reference_dialog form');
    form.find('input[type=text], input[type=number], input[type=password], input:not([type])').val('');
    form.find('input[name=username]').val('Administrator');
    $('#create_cluster_reference_dialog_errors_container').html('');
    form.bind('submit', onSubmit);
    showDialog('create_cluster_reference_dialog', {
      onHide: onHide
    });
    return;

    function onHide() {
      form.unbind('submit', onSubmit);
    }
    var lastGen;
    function onSubmit(e) {
      e.preventDefault();
      var gen = lastGen = {};
      ReplicationsModel.remoteClustersListURICell.getValue(function (url) {
        if (lastGen !== gen) {
          return;
        }
        ReplicationsSection.submitRemoteCluster(url, form);
      });
    }
  },
  startDeleteRemoteCluster: function (name) {
    var remoteCluster;
    this.getRemoteCluster(name, function (_remoteCluster) {
      remoteCluster = _remoteCluster;
      if (!remoteCluster) {
        return;
      }
      genericDialog({text: "Please, confirm deleting remote cluster reference '" + name + "'.",
                     callback: dialogCallback});
    });
    return;

    function dialogCallback(e, name, instance) {
      instance.close();
      if (name != 'ok') {
        return;
      }

      ReplicationsModel.remoteClustersListCell.setValue(undefined);

      $.ajax({
        type: 'DELETE',
        url: remoteCluster.uri,
        success: ajaxCallback,
        errors: ajaxCallback
      });

      function ajaxCallback() {
        ReplicationsModel.refreshReplications();
      }
    }
  },
  startCreateReplication: function () {
    // TODO: disallow create when no remote clusters are defined
    ReplicationForm.instance().startCreate(function (status) {
      ReplicationsModel.refreshReplications();
    });
  },
  startDeleteReplication: function (cancelURI) {
    ThePage.ensureSection("replications");
    var startedDeleteReplication = ReplicationsSection.startedDeleteReplication = {};

    if (startedDeleteReplication !== ReplicationsSection.startedDeleteReplication) {
      // this guards us against a bunch of rapid delete button
      // presses. Only latest delete operation should pass through this gates
      return;
    }
    askDeleteConfirmation(cancelURI);
    return;

    function askDeleteConfirmation(cancelURI) {
      genericDialog({
        header: "Confirm delete",
        text: "Please, confirm deleting this replication",
        callback: function (e, name, instance) {
          instance.close();
          if (name !== 'ok') {
            return;
          }
          doDelete(cancelURI);
        }
      });
    }

    function doDelete(cancelURI) {
      couchReq('DELETE', cancelURI, {}, function () {
        // this is success callback
        ReplicationsModel.refreshReplications();
      }, function (error, status, handleUnexpected) {
        if (status === 404) {
          ReplicationsModel.refreshReplications();
          return;
        }
        return handleUnexpected();
      });
    }
  },
  onEnter: function() {
    ReplicationsModel.refreshReplications(true);
  }
};

configureActionHashParam("deleteReplication", $m(ReplicationsSection, "startDeleteReplication"));
