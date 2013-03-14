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
var SettingsSection = {
  processSave: function(self, continuation) {
    var dialog = genericDialog({
      header: 'Saving...',
      text: 'Saving settings.  Please wait a bit.',
      buttons: {ok: false, cancel: false}});

    var form = $(self);

    var postData = serializeForm(form);

    form.find('.warn li').remove();

    jsonPostWithErrors($(self).attr('action'), postData, function (data, status) {
      if (status != 'success') {
        var ul = form.find('.warn ul');
        _.each(data, function (error) {
          var li = $('<li></li>');
          li.text(error);
          ul.prepend(li);
        });
        $('html, body').animate({scrollTop: ul.offset().top-100}, 250);
        return dialog.close();
      }

      continuation = continuation || function () {
        reloadApp(function (reload) {
          if (data && data.newBaseUri) {
            var uri = data.newBaseUri;
            if (uri.charAt(uri.length-1) == '/')
              uri = uri.slice(0, -1);
            uri += document.location.pathname;
          }
          _.delay(_.bind(reload, null, uri), 1000);
        });
      }

      continuation(dialog);
    });
  },
  init: function () {
    this.tabs = new TabsCell('settingsTabs',
                             '#settings .tabs',
                             '#settings .panes > div',
                             ['update_notifications', 'auto_failover',
                              'email_alerts', 'settings_compaction']);

    UpdatesNotificationsSection.init();
    AutoFailoverSection.init();
    EmailAlertsSection.init();
    AutoCompactionSection.init();
    SampleBucketSection.init();
  },
  onEnter: function () {
    SampleBucketSection.refresh();
    UpdatesNotificationsSection.refresh();
    AutoFailoverSection.refresh();
    EmailAlertsSection.refresh();
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  },
  onLeave: function () {
  },
  renderErrors: function(val, rootNode) {
    if (val!==undefined  && DAL.cells.mode.value==='settings') {
      if (val.errors===null) {
        rootNode.find('.error-container.active').empty().removeClass('active');
        rootNode.find('input.invalid').removeClass('invalid');
        rootNode.find('.save_button').removeAttr('disabled');
      } else {
        // Show error messages on all input fields that contain one
        $.each(val.errors, function(name, message) {
          rootNode.find('.err-'+name).text(message).addClass('active')
            .prev('textarea, input').addClass('invalid');
        });
        // Disable the save button
        rootNode.find('.save_button').attr('disabled', 'disabled');
      }
    }
  }
};

var UpdatesNotificationsSection = {
  buildPhoneHomeThingy: function (statsInfo) {
    var numMembase = 0;
    for (var i in statsInfo.buckets) {
      if (statsInfo.buckets[i].bucketType == "membase")
        numMembase++;
    }
    var nodeStats = {
      os: [],
      uptime: [],
      istats: []
    };
    for(i in statsInfo.pool.nodes) {
      nodeStats.os.push(statsInfo.pool.nodes[i].os);
      nodeStats.uptime.push(statsInfo.pool.nodes[i].uptime);
      nodeStats.istats.push(statsInfo.pool.nodes[i].interestingStats);
    }
    var stats = {
      version: DAL.version,
      componentsVersion: DAL.componentsVersion,
      uuid: DAL.uuid,
      numNodes: statsInfo.pool.nodes.length,
      ram: {
        total: statsInfo.pool.storageTotals.ram.total,
        quotaTotal: statsInfo.pool.storageTotals.ram.quotaTotal,
        quotaUsed: statsInfo.pool.storageTotals.ram.quotaUsed
      },
      hdd: {
        total: statsInfo.pool.storageTotals.hdd.total,
        quotaTotal: statsInfo.pool.storageTotals.hdd.quotaTotal,
        used: statsInfo.pool.storageTotals.hdd.used,
        usedByData: statsInfo.pool.storageTotals.hdd.usedByData
      },
      buckets: {
        total: statsInfo.buckets.length,
        membase: numMembase,
        memcached: statsInfo.buckets.length - numMembase
      },
      counters: statsInfo.pool.counters,
      nodes: nodeStats,
      browser: navigator.userAgent
    };

    return stats;
  },
  launchMissiles: function (statsInfo, launchID) {
    var self = this;
    var iframe = document.createElement("iframe");
    document.body.appendChild(iframe);
    setTimeout(afterDelay, 10);

    // apparently IE8 needs some delay
    function afterDelay() {
      var iwin = iframe.contentWindow;
      var idoc = iwin.document;
      idoc.body.innerHTML = "<form id=launchpad method=POST><textarea id=sputnik name=stats></textarea></form>";
      var form = idoc.getElementById('launchpad');
      var textarea = idoc.getElementById('sputnik');
      form['action'] = self.remote.stats + '?launchID=' + launchID;
      textarea.innerText = JSON.stringify(self.buildPhoneHomeThingy(statsInfo));
      form.submit();
      setTimeout(function () {
        document.body.removeChild(iframe);
      }, 30000);
    }
  },
  init: function () {
    var self = this;

    // All the infos that are needed to send out the statistics
    var statsInfoCell = Cell.computeEager(function (v) {
      return {
        pool: v.need(DAL.cells.currentPoolDetailsCell),
        buckets: v.need(DAL.cells.bucketsListCell)
      };
    });

    var haveStatsInfo = Cell.computeEager(function (v) {
      return !!v(statsInfoCell);
    });

    var modeDefined = Cell.compute(function (v) {
      v.need(DAL.cells.mode);
      return true;
    });

    var phEnabled = Cell.compute(function(v) {
      // only make the GET request when we are logged in
      v.need(modeDefined);
      return future.get({url: "/settings/stats"});
    });
    self.phEnabled = phEnabled;
    phEnabled.equality = _.isEqual;
    phEnabled.keepValueDuringAsync = true;

    self.hasUpdatesCell = Cell.computeEager(function (v) {
      if (!v(modeDefined)) {
        return;
      }
      var enabled = v.need(phEnabled);
      if (!enabled.sendStats) {
        return false;
      }

      if (!v(haveStatsInfo)) {
        return;
      }

      // note: we could've used keepValueDuringAsync option of cell,
      // but then false returned above when sendStats was off would be
      // kept while we're fetching new versions info.
      //
      // So code below keeps old value iff it's some sensible phone
      // home result rather than false
      var nowValue = this.self.value;
      if (nowValue === false) {
        nowValue = undefined;
      }

      return future(function (valueCallback) {
        statsInfoCell.getValue(function (statsInfo) {
          doSendStuff(statsInfo, function () {
            setTimeout(function () {
              self.hasUpdatesCell.recalculateAfterDelay(3600*1000);
            }, 10);
            return valueCallback.apply(this, arguments);
          });
        });
      }, {nowValue: nowValue});

      function doSendStuff(statsInfo, valueCallback) {
        var launchID = DAL.uuid + '-' + (new Date()).valueOf() + '-' + ((Math.random() * 65536) >> 0);
        self.launchMissiles(statsInfo, launchID);
        var valueDelivered = false;

        $.ajax({
          url: self.remote.stats,
          dataType: 'jsonp',
          data: {launchID: launchID, version: DAL.version},
          error: function() {
            valueDelivered = true;
            valueCallback({failed: true});
          },
          timeout: 15000,
          success: function (data) {
            sendStatsSuccess = true;
            valueCallback(data);
          }
        });
        // manual error callback in case timeout & error thing doesn't
        // work
        setTimeout(function() {
          if (!valueDelivered) {
            valueCallback({failed: true});
          }
        }, 15100);
      }
    });

    Cell.subscribeMultipleValues(function (enabled, phoneHomeResult) {
      if (enabled === undefined || phoneHomeResult === undefined) {
        self.renderTemplate(enabled ? enabled.sendStats : false, undefined);
        prepareAreaUpdate($($i('notifications_container')));
        return;
      }
      if (phoneHomeResult.failed) {
        phoneHomeResult = undefined;
      }
      self.renderTemplate(enabled.sendStats, phoneHomeResult);
    }, phEnabled, self.hasUpdatesCell);

    $('#notifications_container').delegate('a.more_info', 'click', function(e) {
      e.preventDefault();
      $('#notifications_container p.more_info').slideToggle();
    }).delegate('input[type=checkbox]', 'change', function() {
      $('#notifications_container .save_button').removeAttr('disabled');
    }).delegate('.save_button', 'click', function() {
      var button = this;
      var sendStatus = $('#notification-updates').is(':checked');

      $.ajax({
        type: 'POST',
        url: '/settings/stats',
        data: {sendStats: sendStatus},
        success: function() {
          $(button).text('Done!').attr('disabled', 'disabled');
          phEnabled.recalculateAfterDelay(2000);
        },
        error: function() {
          genericDialog({
            buttons: {ok: true},
            header: 'Unable to save settings',
            textHTML: 'An error occured, update notifications settings were' +
              ' not saved.'
          });
        }
      });
    });
  },
  //   - sendStats:boolean Whether update notifications are enabled or not
  //   - data:object Consists of everything that comes back from the
  //     proxy that contains the information about software updates. The
  //     object consists of:
  //     - newVersion:undefined|false|string If it is a string a new version
  //       is available, it is false no now version is available. undefined
  //       means that an error occured while retreiving the information
  //     - links:object An object that contains links to the download and
  //       release notes (keys are "download" and "release").
  //     - info:string Some additional information about the new release
  //       (may contain HTML)
  renderTemplate: function(sendStats, data) {
    var newVersion;
    // data might be undefined.
    if (data) {
      newVersion = data.newVersion;
    } else {
      data = {
        newVersion: undefined
      };
    }
    renderTemplate('nav_settings',
                   {enabled: sendStats, newVersion: newVersion},
                   $i('nav_settings_container'));
    renderTemplate('notifications',
                   $.extend(data, {enabled: sendStats,
                                   version: DAL.prettyVersion(DAL.version)}),
                   $i('notifications_container'));
  },
  remote: {
    stats: 'http://ph.couchbase.net/v2',
    email: 'http://ph.couchbase.net/email'
  },
  refresh: function() {
    this.phEnabled.recalculate();
  },
  onEnter: function () {
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  },
  onLeave: function () {
  }
};

var SampleBucketSection = {

  refresh: function() {
    IOCenter.performGet({
      type: 'GET',
      url: '/sampleBuckets',
      dataType: 'json',
      success: function (buckets) {
        var htmlName, tmp, installed = [], available = [];
        _.each(buckets, function(bucket) {
          htmlName = escapeHTML(bucket.name);
          if (bucket.installed) {
            installed.push('<li>' + htmlName + '</li>');
          } else {
            tmp = '<li><input type="checkbox" value="' + htmlName + '" id="sample-' +
              htmlName + '" data-quotaNeeded="'+ bucket.quotaNeeded +
              '" />&nbsp; <label for="sample-' + htmlName + '">' + htmlName +
              '</label></li>';
            available.push(tmp);
          }
        });

        available = (available.length === 0) ?
          '<li>There are no samples available to install.</li>' :
          available.join('');

        installed = (installed.length === 0) ?
          '<li>There are no installed samples.</li>' :
          installed.join('');

        $('#installed_samples').html(installed);
        $('#available_samples').html(available);
      }
    });
  },
  init: function() {

    var self = this;
    var processing = false;
    var form = $("#sample_buckets_form");
    var button = $("#sample_buckets_settings_btn");
    var quotaWarning = $("#sample_buckets_quota_warning");
    var rebalanceWarning = $("#sample_buckets_rebalance_warning");

    var hasBuckets = false;
    var quotaAvailable = false;
    var numServers = false;

    var checkedBuckets = function() {
      return _.map($("#sample_buckets_form").find(':checked'), function(obj) {
        return obj;
      });
    };

    var maybeEnableCreateButton = function() {

      if (processing || numServers === false || quotaAvailable === false) {
        return;
      }

      var storageNeeded = _.reduce(checkedBuckets(), function(acc, obj) {
        return acc + parseInt($(obj).data('quotaneeded'), 10);
      }, 0) * numServers;
      var isStorageAvailable = storageNeeded <= quotaAvailable;

      if (!isStorageAvailable) {
        $('#sampleQuotaRequired')
          .text(Math.ceil(storageNeeded - quotaAvailable) / 1024 / 1024 / numServers);
        quotaWarning.show();
      } else {
        quotaWarning.hide();
      }

      var rebalanceTasks;
      DAL.cells.tasksProgressCell.getValue(function (tasks) {
        rebalanceTasks = _.filter(tasks, function (task) {
          return task.type === 'rebalance' && task.status !== "notRunning";
        });
      });
      rebalanceTasks = rebalanceTasks || [];
      rebalanceWarning[rebalanceTasks.length ? "show" : "hide"]();

      if (hasBuckets && isStorageAvailable && !rebalanceTasks.length) {
        button.removeAttr('disabled');
      } else {
        button.attr('disabled', true);
      }
    };

    $('#sample_buckets_form').bind('change', function() {
      hasBuckets = (checkedBuckets().length > 0);
      maybeEnableCreateButton();
    });

    DAL.cells.serversCell.subscribeValue(function (servers) {
      if (servers) {
        numServers = servers.active.length;
        maybeEnableCreateButton();
      }
    });

    DAL.cells.currentPoolDetailsCell.subscribe(function(pool) {
      var storage = pool.value.storageTotals;
      quotaAvailable = storage.ram.quotaTotal - storage.ram.quotaUsed;
      maybeEnableCreateButton();
    });


    form.bind('submit', function(e) {

      processing = true;
      button.attr('disabled', true).text('Loading');
      e.preventDefault();

      var buckets = JSON.stringify(_.map(checkedBuckets(), function(obj) {
        return obj.value;
      }));

      jsonPostWithErrors('/sampleBuckets/install', buckets, function (simpleErrors, status, errorObject) {
        if (status === 'success') {
          button.text('Create');
          hasBuckets = processing = false;
          SampleBucketSection.refresh();
        } else {
          var errReason = errorObject && errorObject.reason || simpleErrors.join(' and ');
          button.text('Create');
          hasBuckets = processing = false;
          maybeEnableCreateButton();
          genericDialog({
            buttons: {ok: true},
            header: 'Error',
            textHTML: errReason
          });
        }
      }, {
        timeout: 140000
      });

    });
  }
};

var AutoFailoverSection = {
  init: function () {
    var self = this;

    var modeDefined = Cell.compute(function (v) {
      v.need(DAL.cells.mode);
      return true;
    });

    this.autoFailoverEnabled = Cell.compute(function(v) {
      // only make the GET request when we are logged in
      v.need(modeDefined);
      return future.get({url: "/settings/autoFailover"});
    });
    var autoFailoverEnabled = this.autoFailoverEnabled;

    autoFailoverEnabled.subscribeValue(function(val) {
      if (val!==undefined) {
        renderTemplate('auto_failover', val, $i('auto_failover_container'));
        self.toggle(val.enabled);
        //$('#auto_failover_container .save_button')
        //  .attr('disabled', 'disabled');
        $('#auto_failover_enabled').change(function() {
          self.toggle($(this).is(':checked'));
        });
      }
    });

    // we need a second cell for the server screen which is updated
    // more frequently (every 20 seconds if the user is currently on the
    // server screen)
    this.autoFailoverEnabledStatus = Cell.compute(function (v) {
      v.need(DAL.cells.mode);
      // Only poll if we are in the server screen
      if (DAL.cells.mode.value==='servers') {
        return future.get({url: "/settings/autoFailover"});
      }
    });
    this.autoFailoverEnabledStatus.subscribeValue(function(val) {
      if (val!==undefined && DAL.cells.mode.value==='servers') {
        // Hard-code the number of maximum nodes for now
        var afc = $('#auto_failover_count_container');
        if (val.count > 0) {
          afc.show();
        } else {
          afc.hide();
        }
      }
    });

    $('#auto_failover_container')
      .delegate('input', 'keyup', self.validate)
      .delegate('input[type=checkbox], input[type=radio]', 'change',
                self.validate);

    $('#auto_failover_container').delegate('.save_button',
                                           'click', function() {
      var button = this;
      var enabled = $('#auto_failover_enabled').is(':checked');
      var timeout = $('#auto_failover_timeout').val();
      $.ajax({
        type: 'POST',
        url: '/settings/autoFailover',
        data: {enabled: enabled, timeout: timeout},
        success: function() {
          $(button).text('Done!').attr('disabled', 'disabled');
          autoFailoverEnabled.recalculateAfterDelay(2000);
        },
        error: function() {
          genericDialog({
            buttons: {ok: true},
            header: 'Unable to save settings',
            textHTML: 'An error occured, auto-failover settings were ' +
              'not saved.'
          });
        }
      });
    });

    // reset button
    $('.auto_failover_count_reset').live('click', function() {
      var button = $('span', this);

      $.ajax({
        type: 'POST',
        url: '/settings/autoFailover/resetCount',
        success: function() {
         var text = button.text();
          button.text('Done!');
          window.setTimeout(function() {
            button.text(text).parents('div').eq(0).hide();
          }, 3000);
        },
        error: function() {
          genericDialog({
            buttons: {ok: true},
            header: 'Unable to reset the auto-failover quota',
            textHTML: 'An error occured, auto-failover quota was not reset.'
          });
        }
      });
    });
  },
  // Refreshes the auto-failover settings interface
  refresh: function() {
    this.autoFailoverEnabled.recalculate();
  },
  // Refreshes the auto-failover status that is shown in the server screen
  refreshStatus: function() {
    this.autoFailoverEnabledStatus.recalculate();
  },
  // Call this function to validate the form
  validate: function() {
    $.ajax({
      url: "/settings/autoFailover?just_validate=1",
      type: 'POST',
      data: {
        enabled: $('#auto_failover_enabled').is(':checked'),
        timeout: $('#auto_failover_timeout').val()
      },
      complete: function(jqXhr) {
        var val = JSON.parse(jqXhr.responseText);
        SettingsSection.renderErrors(val, $('#auto_failover_container'));
      }
    });
  },
  // Enables the input fields
  enable: function() {
    $('#auto_failover_enabled').attr('checked', 'checked');
    $('#auto_failover_container').find('input[type=text]:disabled')
      .removeAttr('disabled');
  },
  // Disabled input fields (read-only)
  disable: function() {
    $('#auto_failover_enabled').removeAttr('checked');
    $('#auto_failover_container').find('input[type=text]')
      .attr('disabled', 'disabled');
  },
  // En-/disables the input fields dependent on the input parameter
  toggle: function(enable) {
    if(enable) {
      this.enable();
    } else {
      this.disable();
    }
  },
  onEnter: function () {
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  },
  onLeave: function () {
  }
};

var EmailAlertsSection = {
  init: function () {
    var self = this;

    var modeDefined = Cell.compute(function (v) {
      v.need(DAL.cells.mode);
      return true;
    });

    this.emailAlertsEnabled = Cell.compute(function(v) {
      v.need(modeDefined);
      return future.get({url: "/settings/alerts"});
    });
    var emailAlertsEnabled = this.emailAlertsEnabled;

    emailAlertsEnabled.subscribeValue(function(val) {
      if (val!==undefined) {
        val.recipients = val.recipients.join('\n');
        val.alerts = [{
          label: 'Node was auto-failed-over',
          enabled: $.inArray('auto_failover_node', val.alerts)!==-1,
          value: 'auto_failover_node'
        },{
          label: 'Maximum number of auto-failed-over nodes was reached',
          enabled: $.inArray('auto_failover_maximum_reached',
                             val.alerts)!==-1,
          value: 'auto_failover_maximum_reached'
        },{
          label: 'Node wasn\'t auto-failed-over as other nodes are down ' +
            'at the same time',
          enabled: $.inArray('auto_failover_other_nodes_down',
                             val.alerts)!==-1,
          value: 'auto_failover_other_nodes_down'
        },{
          label: 'Node wasn\'t auto-failed-over as the cluster ' +
            'was too small (less than 3 nodes)',
          enabled: $.inArray('auto_failover_cluster_too_small',
                             val.alerts)!==-1,
          value: 'auto_failover_cluster_too_small'
        },{
          label: 'Node\'s IP address has changed unexpectedly',
          enabled: $.inArray('ip', val.alerts)!==-1,
          value: 'ip'
        },{
          label: 'Disk space used for persistent storage has reached ' +
            'at least 90% of capacity',
          enabled: $.inArray('disk', val.alerts)!==-1,
          value: 'disk'
        },{
          label: 'Metadata overhead is more than 50%',
          enabled: $.inArray('overhead', val.alerts)!==-1,
          value: 'overhead'
        },{
          label: 'Bucket memory on a node is entirely used for metadata',
          enabled: $.inArray('ep_oom_errors', val.alerts)!==-1,
          value: 'ep_oom_errors'
        },{
          label: 'Writing data to disk for a specific bucket has failed',
          enabled: $.inArray('ep_oom_errors', val.alerts)!==-1,
          value: 'ep_item_commit_failed'
        }];

        renderTemplate('email_alerts', val, $i('email_alerts_container'));
        self.toggle(val.enabled);

        $('#email_alerts_enabled').change(function() {
          self.toggle($(this).is(':checked'));
        });
      }
    });

    $('#test_email').live('click', function() {
      var testButton = $(this).text('Sending...').attr('disabled', 'disabled');
      var params = $.extend({
        subject: 'Test email from Couchbase Server',
        body: 'This email was sent to you to test the email alert email ' +
          'server settings.'
      }, self.getParams());

      jsonPostWithErrors('/settings/alerts/testEmail', $.param(params), function (data, status) {
        if (status === 'success') {
          testButton.text('Sent!').css('font-weight', 'bold');
          // Increase compatibility with unnamed functions
          window.setTimeout(function() {
            self.resetEmailButton();
          }, 1000);
        } else if (status === 'error') {
          testButton.text('Error!').css('font-weight', 'bold');
        }
      });
    });

    $('#email_alerts_container')
      .delegate('input', 'keyup', function() {
        self.resetEmailButton();
        self.validate(self.getParams());
      })
      .delegate('textarea, input[type=checkbox], input[type=radio]', 'change',
                function() {
        self.resetEmailButton();
        self.validate(self.getParams());
      });

    $('#email_alerts_container').delegate('.save_button',
                                          'click', function() {
      var button = this;
      var params = self.getParams();

      $.ajax({
        type: 'POST',
        url: '/settings/alerts',
        data: params,
        success: function() {
          $(button).text('Done!').attr('disabled', 'disabled');
          emailAlertsEnabled.recalculateAfterDelay(2000);
        },
        error: function() {
          genericDialog({
            buttons: {ok: true},
            header: 'Unable to save settings',
            textHTML: 'An error occured, alerts settings were not saved.'
          });
        }
      });
    });
  },
  // get the parameters from the input fields
  getParams: function() {
    var alerts = $('#email_alerts_alerts input[type=checkbox]:checked')
      .map(function() {
        return this.value;
      }).get();
    var recipients = $('#email_alerts_recipients')
      .val().replace(/\s+/g, ',');
    return {
      enabled: $('#email_alerts_enabled').is(':checked'),
      recipients: recipients,
      sender: $('#email_alerts_sender').val(),
      emailUser: $('#email_alerts_user').val(),
      emailPass: $('#email_alerts_pass').val(),
      emailHost: $('#email_alerts_host').val(),
      emailPort: $('#email_alerts_port').val(),
      emailEncrypt: $('#email_alerts_encrypt').is(':checked'),
      alerts: alerts.join(',')
    };
  },
  // Resets the email button to the default text and state
  resetEmailButton: function() {
    $('#test_email').text('Test Mail').css('font-weight', 'normal')
      .removeAttr('disabled');
  },
  refresh: function() {
    this.emailAlertsEnabled.recalculate();
  },
  // Call this function to validate the form
  validate: function(data) {
    $.ajax({
      url: "/settings/alerts?just_validate=1",
      type: 'POST',
      data: data,
      complete: function(jqXhr) {
        var val = JSON.parse(jqXhr.responseText);
        SettingsSection.renderErrors(val, $('#email_alerts_container'));
      }
    });
  },
  // Enables the input fields
  enable: function() {
    $('#email_alerts_enabled').attr('checked', 'checked');
    $('#email_alerts_container').find('input:disabled')
      .not('#email_alerts_enabled')
      .removeAttr('disabled');
    $('#email_alerts_container').find('textarea').removeAttr('disabled');
  },
  // Disabled input fields (read-only)
  disable: function() {
    $('#email_alerts_enabled').removeAttr('checked');
    $('#email_alerts_container').find('input')
      .not('#email_alerts_enabled')
      .attr('disabled', 'disabled');
    $('#email_alerts_container').find('textarea')
      .attr('disabled', 'disabled');
  },
  // En-/disables the input fields dependent on the input parameter
  toggle: function(enable) {
    if(enable) {
      this.enable();
    } else {
      this.disable();
    }
  },
  onEnter: function () {
  },
  navClick: function () {
    this.onLeave();
    this.onEnter();
  },
  onLeave: function () {
  }
};

var AutoCompactionSection = {
  init: function () {
    var self = this;

    var settingsCell = Cell.needing(DAL.cells.currentPoolDetailsCell).computeEager(function (v, poolDetails) {
      return poolDetails.autoCompactionSettings;
    });
    settingsCell.equality = _.isEqual;
    self.settingsCell = settingsCell;
    var errorsCell = self.errorsCell = new Cell();
    self.errorsCell.subscribeValue($m(self, 'onValidationResult'));
    self.urisCell = new Cell();
    self.formValidationEnabled = new Cell();
    self.formValidationEnabled.setValue(false);

    var container = self.container = $('#compaction_container');

    self.formValidation = undefined;

    DAL.cells.currentPoolDetailsCell.getValue(function (poolDetails) {
      var value = poolDetails.controllers.setAutoCompaction;
      if (!value) {
        BUG();
      }
      AutoCompactionSection.urisCell.setValue(value)
    });

    var formValidation;

    self.urisCell.getValue(function (uris) {
      var validateURI = uris.validateURI;
      var form = container.find("form");
      formValidation = setupFormValidation(form, validateURI, function (status, errors) {
        errorsCell.setValue(errors);
      }, function() {
        return AutoCompactionSection.serializeCompactionForm(form);
      });
      self.formValidationEnabled.subscribeValue(function (enabled) {
        formValidation[enabled ? 'unpause' : 'pause']();
      });
    });

    (function () {
      var spinner;
      settingsCell.subscribeValue(function (val) {
        if (spinner) {
          spinner.remove();
          spinner = null;
        }
        if (val === undefined) {
          spinner = overlayWithSpinner(container);
        }
      });

      container.find('form').bind('submit', function (e) {
        e.preventDefault();
        if (spinner) {
          return;
        }

        self.submit();
      });
    })();

    SettingsSection.tabs.subscribeValue(function (val) {
      if (val !== 'settings_compaction') {
        self.formValidationEnabled.setValue(false);
        return;
      }
      self.errorsCell.setValue({});
      self.fillSettingsForm(function () {
        self.formValidationEnabled.setValue(true);
      });
    });
  },
  serializeCompactionForm: function(form) {
    return serializeForm(form, {
      'databaseFragmentationThreshold[size]': MBtoBytes,
      'viewFragmentationThreshold[size]': MBtoBytes
    });
  },
  preProcessCompactionValues: function(values) {
    var fields = [
      "databaseFragmentationThreshold[size]",
      "databaseFragmentationThreshold[percentage]",
      "viewFragmentationThreshold[size]",
      "viewFragmentationThreshold[percentage]",
      "allowedTimePeriod[fromHour]",
      "allowedTimePeriod[fromMinute]",
      "allowedTimePeriod[toHour]",
      "allowedTimePeriod[toMinute]"
    ];
    _.each(fields, function(key) {
      if (values[key] === 'undefined') {
        delete values[key];
      }
      if (values[key] &&
          (key === 'databaseFragmentationThreshold[size]' ||
           key === 'viewFragmentationThreshold[size]')) {
        values[key] = Math.round(values[key] / 1024 / 1024);
      }
      if (isFinite(values[key]) &&
          (key === 'allowedTimePeriod[fromHour]' ||
           key === 'allowedTimePeriod[fromMinute]' ||
           key === 'allowedTimePeriod[toHour]' ||
           key === 'allowedTimePeriod[toMinute]')) {
        // add leading zero where needed
        values[key] = (values[key] + 100).toString().substring(1);
      }
    });
    return values;
  },
  fillSettingsForm: function (whenDone) {
    var self = this;
    var myGeneration = self.fillSettingsGeneration = new Object();
    self.settingsCell.getValue(function (settings) {
      if (myGeneration !== self.fillSettingsGeneration) {
        return;
      }

      settings = _.clone(settings);
      _.each(settings, function (value, k) {
        if (value instanceof Object) {
          _.each(value, function (subVal, subK) {
            settings[k+'['+subK+']'] = subVal;
          });
        }
      });

      if (self.formDestructor) {
        self.formDestructor();
      }
      settings = AutoCompactionSection.preProcessCompactionValues(settings);
      var form = self.container.find("form");
      setFormValues(form, settings);
      self.formDestructor = setAutoCompactionSettingsFields(form, settings);

      if (whenDone) {
        whenDone();
      }
    });
  },
  submit: function () {
    var self = this;
    var dialog = genericDialog({buttons: {},
                                header: "Saving...",
                                text: "Saving autocompaction settings. Please, wait..."});

    self.urisCell.getValue(function (uris) {
      var data = AutoCompactionSection.serializeCompactionForm(self.container.find("form"));
      jsonPostWithErrors(uris.uri, data, onSubmitResult);
      DAL.cells.currentPoolDetailsCell.setValue(undefined);
      self.formValidationEnabled.setValue(false);
    });
    return;

    function onSubmitResult(simpleErrors, status, errorObject) {
      DAL.cells.currentPoolDetailsCell.invalidate();
      dialog.close();

      if (status == "success") {
        self.errorsCell.setValue({});
        self.fillSettingsForm();
      } else {
        self.errorsCell.setValue(errorObject);
        if (simpleErrors && simpleErrors.length) {
          genericDialog({buttons: {ok: true, cancel: false},
                         header: "Failed To Save Auto-Compaction Settings",
                         text: simpleErrors.join(' and ')});
        }
      }
      self.formValidationEnabled.setValue(true);
    }
  },
  renderError: function (field, error) {
    var fieldClass = field.replace(/\[|\]/g, '-');
    this.container.find('.error-container.err-' + fieldClass).text(error || '')[error ? 'addClass' : 'removeClass']('active');
    this.container.find('[name="' + field + '"]')[error ? 'addClass' : 'removeClass']('invalid');
  },
  onValidationResult: (function () {
    var knownFields = ('name ramQuotaMB replicaNumber proxyPort databaseFragmentationThreshold[percentage] viewFragmentationThreshold[percentage] viewFragmentationThreshold[size] databaseFragmentationThreshold[size] allowedTimePeriod').split(' ');
    _.each(('to from').split(' '), function (p1) {
      _.each(('Hour Minute').split(' '), function (p2) {
        knownFields.push('allowedTimePeriod[' + p1 + p2 + ']');
      });
    });

    return function (result) {
      if (!result) {
        return;
      }
      var self = this;
      var errors = result.errors || {};
      _.each(knownFields, function (name) {
        self.renderError(name, errors[name]);
      });
    };
  })()
};
