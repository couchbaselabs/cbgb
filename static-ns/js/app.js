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
function performSignOut() {
  if (ModalAction.isActive()) {
    $(window).one('modal-action:complete', function () {performSignOut()});
    return;
  }

  $(window).trigger('hashchange'); // this will close all dialogs

  DAL.setAuthCookie(null);
  DAL.cells.mode.setValue(undefined);
  DAL.cells.currentPoolDetailsCell.setValue(undefined);
  DAL.cells.poolList.setValue(undefined);
  DAL.ready = false;

  $(document.body).addClass('auth');

  $('.sign-out-link').hide();
  DAL.onReady(function () {
    if (DAL.login)
      $('.sign-out-link').show();
  });

  _.defer(function () {
    var e = $('#auth_dialog [name=password]').get(0);
    try {e.focus();} catch (ex) {}
  });
}

;(function () {
  var weekDays = "Sun Mon Tue Wed Thu Fri Sat".split(' ');
  var monthNames = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(' ');
  function _2digits(d) {
    d += 100;
    return String(d).substring(1);
  }

  window.formatAlertTStamp = formatAlertTStamp;
  function formatAlertTStamp(mseconds) {
    var date = new Date(mseconds);
    var rv = [weekDays[date.getDay()],
      ' ',
      monthNames[date.getMonth()],
      ' ',
      date.getDate(),
      ' ',
      _2digits(date.getHours()), ':', _2digits(date.getMinutes()), ':', _2digits(date.getSeconds()),
      ' ',
      date.getFullYear()];

    return rv.join('');
  }

  window.formatLogTStamp = function formatLogTStamp(mseconds) {
    var date = new Date(mseconds);
    var rv = [
      "<strong>",
      _2digits(date.getHours()), ':', _2digits(date.getMinutes()), ':', _2digits(date.getSeconds()),
      "</strong> - ",
      weekDays[date.getDay()],
      ' ',
      monthNames[date.getMonth()],
      ' ',
      date.getDate(),
      ', ',
      date.getFullYear()];

    return rv.join('');
  };

  window.formatTime = function formatTime(mseconds) {
    var date = new Date(mseconds);
    var rv = [
      _2digits(date.getHours()), ':',
      _2digits(date.getMinutes()), ':',
      _2digits(date.getSeconds())];
    return rv.join('');
  };


})();

function formatAlertType(type) {
  switch (type) {
  case 'warning':
    return "Warning";
  case 'attention':
    return "Needs Your Attention";
  case 'info':
    return "Informative";
  }
}

var LogsSection = {
  init: function () {
    var active = Cell.needing(DAL.cells.mode).compute(function (v, mode) {
      return (mode === "log") || undefined;
    });

    var logs = Cell.needing(active).compute(function (v, active) {
      return future.get({url: "/logs"});
    });
    logs.keepValueDuringAsync = true;
    logs.subscribe(function (cell) {
      cell.recalculateAfterDelay(30000);
    });

    this.logs = logs;

    var massagedLogs = Cell.compute(function (v) {
      var logsValue = v(logs);
      var stale = v.need(IOCenter.staleness);
      if (logsValue === undefined) {
        if (!stale)
          return;
        logsValue = {list: []};
      }
      return _.extend({}, logsValue, {stale: stale});
    });

    renderCellTemplate(massagedLogs, 'logs', {
      valueTransformer: function (value) {
        var list = value.list || [];
        return _.clone(list).reverse();
      }
    });

    massagedLogs.subscribeValue(function (massagedLogs) {
      if (massagedLogs === undefined)
        return;
      var stale = massagedLogs.stale;
      $('#logs .staleness-notice')[stale ? 'show' : 'hide']();
    });
  },
  onEnter: function () {
  },
  navClick: function () {
    if (DAL.cells.mode.value == 'log')
      this.logs.recalculate();
  },
  domId: function (sec) {
    return 'logs';
  }
}

var ThePage = {
  sections: {overview: OverviewSection,
             servers: ServersSection,
             analytics: AnalyticsSection,
             buckets: BucketsSection,
             views: ViewsSection,
             replications: ReplicationsSection,
             documents: DocumentsSection,
             log: LogsSection,
             settings: SettingsSection},

  coming: {alerts:true},

  currentSection: null,
  currentSectionName: null,
  signOut: function () {
    performSignOut();
  },
  ensureSection: function (section) {
    if (this.currentSectionName != section)
      this.gotoSection(section);
  },
  gotoSection: function (section) {
    if (!(this.sections[section])) {
      throw new Error('unknown section:' + section);
    }
    if (this.currentSectionName == section) {
      if ('navClick' in this.currentSection)
        this.currentSection.navClick();
      else
        this.currentSection.onEnter();
    } else
      setHashFragmentParam('sec', section);
  },
  initialize: function () {
    var self = this;

    function preventKeys(e) {
      if (e.keyCode === 27) { //esc
        e.preventDefault();
      }
    }
    $(document).keydown(preventKeys).keypress(preventKeys);

    _.each(_.uniq(_.values(this.sections)), function (sec) {
      if (sec.init)
        sec.init();
    });

    initAlertsSubscriber();

    DAL.cells.mode.subscribeValue(function (sec) {
      $('.currentNav').removeClass('currentNav');
      $('#switch_' + sec).parent('li').addClass('currentNav');

      var oldSection = self.currentSection;
      var currentSection = self.sections[sec];
      if (!currentSection) {
        return;
      }
      self.currentSectionName = sec;
      self.currentSection = currentSection;

      var secId = sec;
      if (currentSection.domId != null) {
        secId = currentSection.domId(sec);
      }

      if (self.coming[sec] == true && window.location.href.indexOf("FORCE") < 0) {
        secId = 'coming';
      }

      $('#mainPanel > div:not(.notice)').css('display', 'none');
      $('#'+secId).css('display','block');

      _.defer(function () {
        if (oldSection && oldSection.onLeave)
          oldSection.onLeave();
        self.currentSection.onEnter();
        $(window).trigger('sec:' + sec);
      });
    });

    DAL.onReady(function () {
      if (DAL.login) {
        $('.sign-out-link').show();
      }
    });

    watchHashParamChange('sec', 'overview', function (sec) {
      DAL.switchSection(sec);
    });
    _.defer(function() {
      $(window).trigger('hashchange');
    });
  }
};

function hideAuthForm() {
  $(document.body).removeClass('auth');
}

function loginFormSubmit() {
  var login = $('#login_form [name=login]').val();
  var password = $('#login_form [name=password]').val();
  var spinner = overlayWithSpinner('#login_form', false);
  $('#auth_dialog .alert_red').hide();
  $('#login_form').addClass('noform');
  DAL.performLogin(login, password, function (status) {
    spinner.remove();
    $('#login_form').removeClass('noform');

    if (status == 'success') {
      hideAuthForm();
      // don't keep credentials in DOM tree
      $('#login_form [name=password]').val('');
      return;
    }

    $('#auth_failed_message').show();
  });
  return false;
}

var SetupWizard = {
  show: function(page, opt, isContinuation) {
    opt = opt || {};
    page = page || "welcome";

    var pageNames = _.keys(SetupWizard.pages);

    for (var i = 0; i < pageNames.length; i++) {
      if (page == pageNames[i]) {
        var rv;
        if (!isContinuation && SetupWizard.pages[page]) {
          rv = SetupWizard.pages[page]('self', 'init_' + page, opt);
        }
        // if page is in continuation passing style, call it
        // passing continuation and return.  This allows page to do
        // async computation and then resume dialog page switching
        if (rv instanceof Function) {
          $('body, html').css('cursor', 'wait');

          return rv(function () {

            $('body, html').css('cursor', '');
            // we don't pass real continuation, we just call ourselves again
            SetupWizard.show(page, opt, true);
          });
        }
        $(document.body).addClass('init_' + page);
        $('html, body').animate({scrollTop:0},250);
        _.defer(function () {
          var element = $('.focusme:visible').get(0)
          if (!element) {
            return;
          }
          try {element.focus();} catch (e) {}
        });
      }
    }

    if (page == 'done')
      DAL.enableSections();

    // Hiding other pages in a 2nd loop to prevent "flashing" between pages
    for (var i = 0; i < pageNames.length; i++) {
      if (page != pageNames[i]) {
        $(document.body).removeClass('init_' + pageNames[i]);
      }
    }

    if (page == 'done')
      return;

    var notices = [];
    $('#notice_container > *').each(function () {
      var text = $.data(this, 'notice-text');
      if (!text)
        return;
      notices.push(text);
    });
    if (notices.length) {
      $('#notice_container').empty();
      alert(notices.join("\n\n"));
    }
  },
  panicAndReload: function() {
    alert('Failed to get initial setup data from server. Cannot continue.' +
          ' Would you like to attempt to reload the web console?  This may fail if the server is not running.');
    reloadApp();
  },
  doClusterJoin: function () {
    var form = $('#init_cluster_form');

    var errorsContainer = form.parent().find('.join_cluster_dialog_errors_container');
    errorsContainer.hide();

    var data = ServersSection.validateJoinClusterParams(form);
    if (data.length) {
      renderTemplate('join_cluster_dialog_errors', data, errorsContainer[0]);
      errorsContainer.show();
      return;
    }

    var arr = data.hostname.split(':');
    data.clusterMemberHostIp = arr[0];
    data.clusterMemberPort = arr[1] ? arr[1] : '8091';
    delete data.hostname;

    var overlay = overlayWithSpinner($('#init_cluster_dialog'), '#EEE');
    jsonPostWithErrors('/node/controller/doJoinCluster', $.param(data), function (errors, status) {
      if (status != 'success') {
        overlay.remove();
        renderTemplate('join_cluster_dialog_errors', errors, errorsContainer[0]);
        errorsContainer.show();
        return;
      }

      DAL.setAuthCookie(data.user, data.password);
      DAL.tryNoAuthLogin();
      overlay.remove();
      displayNotice('This server has been associated with the cluster and will join on the next rebalance operation.');
    });
  },
  pages: {
    bucket_dialog: function () {
      var spinner;
      var timeout = setTimeout(function () {
        spinner = overlayWithSpinner('#init_bucket_dialog');
      }, 50);
      $.ajax({url: '/pools/default/buckets/default',
              success: continuation,
              error: continuation,
              dataType: 'json'});
      function continuation(data, status) {
        if (status != 'success') {
          $.ajax({type:'GET', url:'/nodes/self', dataType: 'json',
                  error: function () {
                    SetupWizard.panicAndReload();
                  },
                  success: function (nodeData) {
                    data = {uri: '/pools/default/buckets',
                            bucketType: 'membase',
                            authType: 'sasl',
                            quota: {
                              rawRAM: nodeData.storageTotals.ram.quotaTotal -
                                nodeData.storageTotals.ram.quotaUsed},
                            replicaNumber: 1,
                            replicaIndex: false};
                    continuation(data, 'success');
                  }});
          return;
        }
        if (spinner)
          spinner.remove();
        clearTimeout(timeout);
        var initValue = _.extend(data, {
          uri: '/controller/setupDefaultBucket'
        });
        var dialog = new BucketDetailsDialog(initValue, true,
                                             {id: 'init_bucket_dialog',
                                              refreshBuckets: function (b) {b()},
                                              onSuccess: function () {
                                                dialog.cleanup();
                                                SetupWizard.show('update_notifications');
                                              }});
        var cleanupBack = dialog.bindWithCleanup($('#step-init-bucket-back'),
                                                 'click',
                                                 function () {
                                                   dialog.cleanup();
                                                   SetupWizard.show('sample_buckets');
                                                 });
        dialog.cleanups.push(cleanupBack);
        dialog.startForm();
      }
    },
    secure: function(node, pagePrefix, opt) {
      var parentName = '#' + pagePrefix + '_dialog';

      $(parentName + ' div.config-bottom button#step-5-finish').unbind('click').click(function (e) {
        e.preventDefault();
        $('#init_secure_form').submit();
      });
      $(parentName + ' div.config-bottom button#step-5-back').unbind('click').click(function (e) {
        e.preventDefault();
        SetupWizard.show("update_notifications");
      });

      var form = $(parentName + ' form').unbind('submit');
      _.defer(function () {
        $(parentName).find('[name=password]')[0].focus();
      });
      form.submit(function (e) {
        function displayTryAgainDialog(options) {
          if (options.callback || options.buttons) {
            BUG();
          }
          options.buttons = {cancel: false, ok: true};
          options.callback = function (e, name, instance) {
            instance.close();
            try {
              var element = parent.find('[name=password]').get(0);
              element.focus();
              element.setSelectionRange(0, String(element.value).length);
            } catch (ignored) {}
          }
          return genericDialog(options);
        }

        e.preventDefault();

        var parent = $(parentName)

        var user = parent.find('[name=username]').val();
        var pw = parent.find('[name=password]').val();
        var vpw = $($i('secure-password-verify')).val();
        if (pw == null || pw == "") {
          displayTryAgainDialog({
            header: 'Please try again',
            text: 'A password of at least six characters is required.'
          });
          return;
        }
        if (pw !== vpw) {
          displayTryAgainDialog({
            header: 'Please try again',
            text: '\'Password\' and \'Verify Password\' do not match'
          });
          return;
        }

        SettingsSection.processSave(this, function (dialog) {
          DAL.performLogin(user, pw, function () {
            SetupWizard.show('done');

            if (user != null && user != "") {
              $('.sign-out-link').show();
            }

            dialog.close();
          });
        });
      });
    },
    welcome: function(node, pagePrefix, opt) {
      $('#init_welcome_dialog input.next').click(function (e) {
        e.preventDefault();
        SetupWizard.show("cluster");
      });
    },

    cluster: function (node, pagePrefix, opt) {
      var dialog = $('#init_cluster_dialog');
      var resourcesObserver;

      $('#join-cluster').click(function (e) {
        $('.login-credentials').slideDown();
        $('.memory-quota').slideUp();
        $('#init_cluster_dialog_memory_errors_container').slideUp();
      });
      $('#no-join-cluster').click(function (e) {
        $('.memory-quota').slideDown();
        $('.login-credentials').slideUp();
      });

      // we return function signaling that we're not yet ready to show
      // our page of wizard (no data to display in the form), but will
      // be at one point. SetupWizard.show() will call us immediately
      // passing us it's continuation. This is partial continuation to
      // be more precise
      return function (continueShowDialog) {
        dialog.find('.quota_error_message').hide();

        $.ajax({type:'GET', url:'/nodes/self', dataType: 'json',
                success: dataCallback, error: dataCallback});

        function dataCallback(data, status) {
          if (status != 'success') {
            return SetupWizard.panicAndReload();
          }

          $('#init_cluster_dialog .when-enterprise').toggle(!!DAL.isEnterprise);

          // we have node data and can finally display our wizard page
          // and pre-fill the form
          continueShowDialog();

          $('#step-2-next').click(onSubmit);
          dialog.find('form').submit(onSubmit);

          _.defer(function () {
            if ($('#join-cluster')[0].checked)
              $('.login-credentials').show();
          });

          var m = data['memoryQuota'];
          if (m == null || m == "none") {
            m = "";
          }

          dialog.find('[name=quota]').val(m);

          data['node'] = data['node'] || node;

          var storageTotals = data.storageTotals;

          var totalRAMMegs = Math.floor(storageTotals.ram.total/Math.Mi);

          dialog.find('[name=dynamic-ram-quota]').val(Math.floor(storageTotals.ram.quotaTotal / Math.Mi));
          dialog.find('.ram-total-size').text(totalRAMMegs + ' MB');
          var ramMaxMegs = Math.max(totalRAMMegs - 1024,
                                    Math.floor(storageTotals.ram.total * 4 / (5 * Math.Mi)));
          dialog.find('.ram-max-size').text(ramMaxMegs);

          var firstResource = data.storage.hdd[0];
          var diskTotalGigs = Math.floor((storageTotals.hdd.total - storageTotals.hdd.used) / Math.Gi);

          var dbPath;
          var dbTotal;

          var ixPath;
          var ixTotal;

          dbPath = dialog.find('[name=db_path]');
          ixPath = dialog.find('[name=index_path]');

          dbTotal = dialog.find('.total-db-size');
          ixTotal = dialog.find('.total-index-size');

          function updateTotal(node, total) {
            node.text(escapeHTML(total) + ' GB');
          }

          dbPath.val(escapeHTML(firstResource.path));
          ixPath.val(escapeHTML(firstResource.path));

          updateTotal(dbTotal, diskTotalGigs);
          updateTotal(ixTotal, diskTotalGigs);

          var hddResources = data.availableStorage.hdd;
          var mountPoints = new MountPoints(data, _.pluck(hddResources, 'path'));

          var prevPathValues = [];

          function maybeUpdateTotal(pathNode, totalNode) {
            var pathValue = pathNode.val();

            if (pathValue == prevPathValues[pathNode]) {
              return;
            }

            prevPathValues[pathNode] = pathValue;
            if (pathValue == "") {
              updateTotal(totalNode, 0);
              return;
            }

            var rv = mountPoints.lookup(pathValue);
            var pathResource = ((rv != null) && hddResources[rv]);

            if (!pathResource) {
              pathResource = {path:"/", sizeKBytes: 0, usagePercent: 0};
            }

            var totalGigs =
              Math.floor(pathResource.sizeKBytes *
                         (100 - pathResource.usagePercent) / 100 / Math.Mi);
            updateTotal(totalNode, totalGigs);
          }

          resourcesObserver = dialog.observePotentialChanges(
            function () {
              maybeUpdateTotal(dbPath, dbTotal);
              maybeUpdateTotal(ixPath, ixTotal);
            });
        }
      }

      // cleans up all event handles
      function onLeave() {
        $('#step-2-next').unbind();
        dialog.find('form').unbind();
        if (resourcesObserver)
          resourcesObserver.stopObserving();
      }

      var saving = false;

      function onSubmit(e) {
        e.preventDefault();
        if (saving) {
          return;
        }

        dialog.find('.warning').hide();

        var dbPath = dialog.find('[name=db_path]').val() || "";
        var ixPath = dialog.find('[name=index_path]').val() || "";

        var m = dialog.find('[name=dynamic-ram-quota]').val() || "";
        if (m == "") {
          m = "none";
        }

        var pathErrorsContainer = dialog.find('.init_cluster_dialog_errors_container');
        var memoryErrorsContainer = $('#init_cluster_dialog_memory_errors_container');
        pathErrorsContainer.hide();
        memoryErrorsContainer.hide();

        var spinner = overlayWithSpinner(dialog);
        saving = true;

        jsonPostWithErrors('/nodes/' + node + '/controller/settings',
                           $.param({path: dbPath,
                                    index_path: ixPath}),
                           afterDisk);

        var diskArguments;

        function afterDisk() {
          // remember our arguments so that we can display validation
          // errors later. We're doing that to display validation errors
          // from memory quota and disk path posts simultaneously
          diskArguments = arguments;
          if ($('#no-join-cluster')[0].checked) {
            jsonPostWithErrors('/pools/default',
                               $.param({memoryQuota: m}),
                               memPost);
            return;
          }

          if (handleDiskStatus.apply(null, diskArguments))
            SetupWizard.doClusterJoin();
        }

        function handleDiskStatus(data, status) {
          saving = false;
          spinner.remove();
          var ok = (status == 'success')
          if (!ok) {
            renderTemplate('join_cluster_dialog_errors', data, pathErrorsContainer[0]);
            pathErrorsContainer.show();
          }
          return ok;
        }

        function memPost(data, status) {
          var ok = handleDiskStatus.apply(null, diskArguments);

          if (status == 'success') {
            if (ok) {
              BucketsSection.refreshBuckets();
              SetupWizard.show("sample_buckets");
              onLeave();
            }
          } else {
            memoryErrorsContainer.text(data.join(' and '));
            memoryErrorsContainer.show();
          }
        }
      }
    },
    update_notifications: function(node, pagePrefix, opt) {
      var dialog = $('#init_update_notifications_dialog');
      var emailField = $('#init-join-community-email').need(1);
      var formObserver;

      $('#update_notifications_errors_container').hide();

      dialog.find('a.more_info').unbind('click').click(function(e) {
        e.preventDefault();
        dialog.find('p.more_info').slideToggle();
      });
      dialog.find('button.back').unbind('click').click(function (e) {
        e.preventDefault();
        onLeave();
        SetupWizard.show("bucket_dialog");
      });

      dialog.find('button.next').unbind('click').click(onNext);
      dialog.find('form').unbind('submit').bind('submit', function (e) {
        e.preventDefault();
        dialog.find('button.next').trigger('click');
      });

      dialog.find('.when-enterprise').toggle(!!DAL.isEnterprise);
      dialog.find('.when-community').toggle(!DAL.isEnterprise);

      setTimeout(function () {
        try {
          emailField[0].focus();
        } catch (e) {
          //ignore
        }
      }, 10);

      formObserver = dialog.observePotentialChanges(emailFieldValidator);

      var appliedValidness = {};
      function applyValidness(validness) {
        _.each(validness, function (v, k) {
          if (appliedValidness[k] === v) {
            return;
          }
          $($i(k))[v ? 'addClass' : 'removeClass']('invalid');
          appliedValidness[k] = v;
        });
      }

      var emailValue;
      var emailInvalidness;

      function validateEmailField() {
        var newEmailValue = $.trim(emailField.val());
        if (emailValue !== newEmailValue) {
          emailValue = newEmailValue;
          emailInvalidness = (newEmailValue !== '' && !HTML5_EMAIL_RE.exec(newEmailValue));
        }
        return emailInvalidness;
      }

      var invalidness = {};
      function emailFieldValidator() {
        var emailId = emailField.attr('id');
        invalidness[emailId] = validateEmailField();
        applyValidness(invalidness);
      }

      var sending;

      // Go to next page. Send off email address if given and apply settings
      function onNext(e) {
        e.preventDefault();
        if (sending) {
          // this prevents double send caused by submitting form via
          // hitting Enter
          return;
        }
        $('#update_notifications_errors_container').hide();

        var errors = [];
        var formObserverResult;

        if (formObserver) {
          formObserver.callback() || {};
        }

        if (emailInvalidness) {
          errors.push("Email appears to be invalid");
        }

        if (DAL.isEnterprise) {
          var termsChecked = !!$('#init-join-terms').attr('checked');
          if (!termsChecked) {
            errors.push("Terms and conditions need to be accepted in order to continue");
          }
        }

        if (errors.length) {
          var invalidFields = dialog.find('.invalid');
          renderTemplate('update_notifications_errors', errors);
          $('#update_notifications_errors_container').show();
          if (invalidFields.length && !_.include(invalidFields, document.activeElement)) {
            setTimeout(function () {try {invalidFields[0].focus();} catch (e) {}}, 10);
          }
          return;
        }

        var email = $.trim(emailField.val());
        if (email!=='') {
          // Send email address. We don't care if notifications were enabled
          // or not.
          $.ajax({
            url: UpdatesNotificationsSection.remote.email,
            dataType: 'jsonp',
            data: {email: email,
                   firstname: $.trim($('#init-join-community-firstname').val()),
                   lastname: $.trim($('#init-join-community-lastname').val()),
                   company: $.trim($('#init-join-community-company').val()),
                   version: DAL.version || "unknown"},
            success: function () {},
            error: function () {}
          });
        }

        var sendStatus = $('#init-notifications-updates-enabled').is(':checked');
        sending = true;
        var spinner = overlayWithSpinner(dialog);
        jsonPostWithErrors(
          '/settings/stats',
          $.param({sendStats: sendStatus}),
          function () {
            spinner.remove();
            sending = false;
            onLeave();
            SetupWizard.show("secure");
          }
        );
      }

      // cleans up all event handles
      function onLeave() {
        if (formObserver) {
          formObserver.stopObserving();
        }
      }
    },

    sample_buckets: function(node, pagePrefix, opt) {

      var spinner;
      var timeout = setTimeout(function() {
        spinner = overlayWithSpinner($('#init_sample_buckets_dialog'), '#EEE');
      }, 20);

      var dialog = $('#init_sample_buckets_dialog');
      var back = dialog.find('button.back');
      var next = dialog.find('button.next');

      _.defer(function () {
        try {next[0].focus();} catch (e) {}
      });

      function checkedBuckets() {
        return _.map(dialog.find(':checked'), function(obj) {
          return $(obj).val();
        });
      }

      function enableForm() {
        dialog.add(next).add(back).css('cursor', 'auto').attr('disabled', false);
      }

      back.unbind('click').click(function (e) {
        e.preventDefault();
        SetupWizard.show('cluster');
      });

      next.unbind('click').click(function (e) {

        e.preventDefault();
        dialog.add(next).add(back).css('cursor', 'wait').attr('disabled', true);

        var complete = function() {
          enableForm();
          SetupWizard.show("bucket_dialog");
        };

        var buckets = checkedBuckets();
        var json = JSON.stringify(buckets);

        if (buckets.length === 0) {
          complete();
          return;
        }

        var loading = genericDialog({
          buttons: {ok: false, cancel: false},
          header: 'Please wait while the sample buckets are loaded.',
          textHTML: '',
          showCloseButton: false
        });
        overlayWithSpinner(loading.dialog);

        jsonPostWithErrors('/sampleBuckets/install', json, function (simpleErrors, status, errorObject) {
          if (status === 'success') {
            loading.close();
            complete();
          } else {
            var errReason = errorObject && errorObject.reason || simpleErrors.join(' and ');
            loading.close();
            enableForm();
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

      $.get('/sampleBuckets', function(buckets) {
        var tmp, htmlName, installed = [], available = [];
        _.each(buckets, function(bucket) {
          htmlName = escapeHTML(bucket.name);
          if (bucket.installed) {
            installed.push('<li>' + htmlName + '</li>');
          } else {
            tmp = '<li><input type="checkbox" value="' + htmlName +
              '" id="setup-sample-' + htmlName + '" />&nbsp; ' +
              '<label for="setup-sample-' + htmlName + '">' +
              htmlName + '</label></li>';
            available.push(tmp);
          }
        });

        available = (available.length === 0) ?
          '<li>There are no samples available to install.</li>' :
          available.join('');

        installed = (installed.length === 0) ?
          '<li>There are no installed samples.</li>' :
          installed.join('');

        $('#setup_installed_samples').html(installed);
        $('#setup_available_samples').html(available);

        enableForm();

        clearTimeout(timeout);
        if (spinner) {
          spinner.remove();
        }
      });

    }
  }
};

$(function () {
  $(document.body).removeClass('nojs');

  _.defer(function () {
    var e = $('#auth_dialog [name=login]').get(0);
    try {e.focus();} catch (ex) {}
  });

  $('#login_form').bind('submit', function (e) {
    e.preventDefault();
    loginFormSubmit();
  });

  if ($.cookie('rf')) {
    displayNotice('An error was encountered when requesting data from the server.  ' +
                  'The console has been reloaded to attempt to recover.  There ' +
                  'may be additional information about the error in the log.');
    DAL.onReady(function () {
      $.cookie('rf', null);
      if ('sessionStorage' in window && window.sessionStorage.reloadCause) {
        var text = "Browser client XHR failure encountered. (age: "
          + ((new Date()).valueOf() - sessionStorage.reloadTStamp)+")  Diagnostic info:\n";
        postClientErrorReport(text + window.sessionStorage.reloadCause);
        delete window.sessionStorage.reloadCause;
        delete window.sessionStorage.reloadTStamp;
      }
    });
  }

  ThePage.initialize();

  DAL.onReady(function () {
    $(window).trigger('hashchange');
  });

  try {
    if (DAL.tryNoAuthLogin()) {
      hideAuthForm();
    }
  } finally {
    try {
      $('#auth_dialog .spinner').remove();
    } catch (__ignore) {}
  }

  if (!DAL.login && $('#login_form:visible').length) {
    var backdoor =
      (function () {
        var href = window.location.href;
        var match = /\?(.*?)(?:$|#)/.exec(href);
        var rv = false;
        if (match && /(&|^)na(&|$)/.exec(match[1]))
          rv = true;
        return rv;
      })();
    if (backdoor) {
      var login = $('#login_form [name=login]').val('Administrator');
      var password = $('#login_form [name=password]').val('asdasd');
      loginFormSubmit();
    }
  }
});

function showAbout() {
  function updateVersion() {
    var components = DAL.componentsVersion;
    if (components)
      $('#about_versions').text("Version: " + DAL.prettyVersion(components['ns_server']));
    else {
      $.get('/versions', function (data) {
        DAL.componentsVersion = data.componentsVersion;
        updateVersion();
      }, 'json')
    }

    var poolDetails = DAL.cells.currentPoolDetailsCell.value || {nodes:[]};
    var nodesCount = poolDetails.nodes.length;
    if (nodesCount >= 0x100)
      nodesCount = 0xff;

    var buckets = DAL.cells.bucketsListCell.value;
    if (buckets !== undefined) {
      var bucketsCount = buckets.length;
      if (bucketsCount >= 100)
        bucketsCount = 99;

      var memcachedBucketsCount = buckets.byType.memcached.length;
      var membaseBucketsCount = buckets.byType.membase.length;

      if (memcachedBucketsCount >= 0x10)
        memcachedBucketsCount = 0xf;
      if (membaseBucketsCount >= 0x10)
        membaseBucketsCount = 0x0f;

      var date = (new Date());

      var magicString = [
        integerToString(0x100 + poolDetails.nodes.length, 16).slice(1)
          + integerToString(date.getMonth()+1, 16),
        integerToString(100 + bucketsCount, 10).slice(1)
          + integerToString(memcachedBucketsCount, 16),
        integerToString(membaseBucketsCount, 16)
          + date.getDate()
      ];
      $($i('cluster_state_id')).text(magicString.join('-'));
    } else {
      $($i('cluster_state_id')).html('Please login to view your ' +
          'Cluster State ID.');
    }
  }
  updateVersion();
  showDialog('about_server_dialog',
      {title: $('#about_server_dialog .config-top').hide().html()});
}

function displayNotice(text, isError) {
  var div = $('<div></div>');
  var tname = 'notice';
  if (isError || (isError === undefined && text.indexOf('error') >= 0)) {
    tname = 'noticeErr';
  }
  renderTemplate(tname, {text: text}, div.get(0));
  $.data(div.children()[0], 'notice-text', text);
  $('#notice_container').prepend(div.children());
  ThePage.gotoSection("overview");
}

$('.notice').live('click', function () {
  var self = this;
  $(self).fadeOut('fast', function () {
    $(self).remove();
  });
});

$('.tooltip').live('click', function (e) {
  e.preventDefault();
  $('.tooltip_msg', this).stop(true, true).fadeIn();
}).live('mouseleave', function (e) {
  $('.tooltip_msg', this).stop(true, true).fadeOut();
})
.delegate('.tooltip_msg', 'mouseenter', function (e) {
  $(this).stop(true, true).fadeIn(0);
});

configureActionHashParam('visitSec', function (sec) {
  ThePage.gotoSection(sec);
});

$(function () {
  var container = $('.io-error-notice');
  var timeoutId;
  function renderStatus() {
    if (timeoutId != null) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }

    var status = IOCenter.status.value;

    if (status.healthy) {
      container.hide();
      return;
    }

    container.show();

    if (status.repeating) {
      container.text('Repeating failed XHR request...');
      return
    }

    var now = (new Date()).getTime();
    var repeatIn = Math.max(0, Math.floor((status.repeatAt - now)/1000));
    container.html('Lost connection to server at ' + window.location.host + '. Repeating in ' + repeatIn + ' seconds. <a href="#">Retry now</a>');
    var delay = Math.max(200, status.repeatAt - repeatIn*1000 - now);
    timeoutId = setTimeout(renderStatus, delay);
  }

  container.find('a').live('click', function (e) {
    e.preventDefault();
    IOCenter.forceRepeat();
  });

  IOCenter.status.subscribeValue(renderStatus);
});

$(function () {
  if (!('AllImages' in window))
    return;
  var images = window['AllImages']

  var body = $(document.body);
  _.each(images, function (path) {
    var j = $(new Image());
    j[0].src = path;
    j.css('display', 'none');
    j.appendTo(body);
  });
});


function runInternalSettingsDialog() {
  var dialog = $('#internal_settings_dialog');
  var form = dialog.find("form");
  showDialog(dialog, {
    closeOnEscape: true,
    title: 'Tweak internal settings',
    onHide: onHide
  });

  var cleanups = [];
  var firedSubmit = false;
  var spinner;
  setTimeout(start, 10);
  return;

  function start() {
    spinner = overlayWithSpinner(form);
    $.getJSON("/internalSettings", populateForm);
  }

  function populateForm(data) {
    setFormValues(form, data);
    spinner.remove();
    var undoBind = BucketDetailsDialog.prototype.bindWithCleanup(form, "submit", onSubmit);
    cleanups.push(undoBind);
  }

  function onSubmit(ev) {
    ev.preventDefault();
    if (firedSubmit) {
      return;
    }
    firedSubmit = true;
    var data = serializeForm(form);
    var secondSpinner = overlayWithSpinner(dialog, undefined, "Saving...");
    $.post('/internalSettings', data, submittedOk);
    cleanups.push(function () {
      secondSpinner.remove();
    });
  }

  function submittedOk() {
    hideDialog(dialog);
  }

  function onHide() {
    _.each(cleanups, function (c) {c()});
  }
}


function initAlertsCells(ns, poolDetailsCell) {
  ns.rawAlertsCell = Cell.compute(function (v) {
    return v.need(poolDetailsCell).alerts;
  }).name("rawAlertsCell");
  ns.rawAlertsCell.equality = _.isEqual;

  // this cells adds timestamps to each alert message each message is
  // added timestamp when it was first seen. With assumption that
  // alert messages are added to the end of alerts array
  //
  // You can see that implementation is using it's own past result if
  // past messages array are prefix of current messages array to
  // achieve this behavior.
  //
  // Also note that undefined rawAlertsCell leads this cell to
  // preserve it's past value. So that poolDetailsCell recomputations
  // do not 'reset history'
  ns.stampedAlertsCell = Cell.compute(function (v) {
    var oldStampedAlerts = this.self.value || [];
    var newRawAlerts = v(ns.rawAlertsCell);
    if (!newRawAlerts) {
      return oldStampedAlerts;
    }
    var i;
    if (oldStampedAlerts.length > newRawAlerts.length) {
      oldStampedAlerts = [];
    }
    for (i = 0; i < oldStampedAlerts.length; i++) {
      if (oldStampedAlerts[i][1] != newRawAlerts[i]) {
        break;
      }
    }
    var rv = (i == oldStampedAlerts.length) ? _.clone(oldStampedAlerts) : [];
    _.each(newRawAlerts.slice(rv.length), function (msg) {
      rv.push([new Date(), msg]);
    });
    return rv;
  }).name("stampedAlertsCell");
  ns.stampedAlertsCell.equality = _.isEqual;

  ns.alertsAndSilenceURLCell = Cell.compute(function (v) {
    return {
      stampedAlerts: v.need(ns.stampedAlertsCell),
      alertsSilenceURL: v.need(poolDetailsCell).alertsSilenceURL
    };
  }).name("alertsAndSilenceURLCell");
  ns.alertsAndSilenceURLCell.equality = _.isEqual;
}


// uses /pools/default to piggyback any user alerts we want to show
function initAlertsSubscriber() {
  var visibleDialog;

  var cells = {};
  window.alertsCells = cells;
  initAlertsCells(cells, DAL.cells.currentPoolDetailsCell);

  cells.alertsAndSilenceURLCell.subscribeValue(function (alertsAndSilenceURL) {
    if (!alertsAndSilenceURL) {
      return;
    }
    var alerts = alertsAndSilenceURL.stampedAlerts;
    var alertsSilenceURL = alertsAndSilenceURL.alertsSilenceURL;
    if (!alerts.length) {
      return;
    }

    if (visibleDialog) {
      visibleDialog.close();
    }


    var alertsMsg = _.map(alerts, function (alertItem) {
      var tstamp = "<strong>["+escapeHTML(formatTime(alertItem[0]))+"]</strong>";
      return tstamp + " - " + escapeHTML(alertItem[1]);
    }).join("<br />");

    _.defer(function () {
      // looks like we cannot reuse dialog immediately if it was
      // closed via visibleDialog.close, thus deferring a bit
      visibleDialog = genericDialog({
        buttons: {ok: true},
        header: "Alert",
        textHTML: alertsMsg,
        dontCloseOnHashchange: true,
        callback: function (e, btn, thisDialog) {
          thisDialog.close();
          if (thisDialog !== visibleDialog) {
            BUG("thisDialog != visibleDialog");
          }
          visibleDialog = null;
          DAL.cells.currentPoolDetailsCell.setValue(undefined);
          $.ajax({url: alertsSilenceURL,
                  type: 'POST',
                  data: '',
                  timeout: 5000,
                  success: done,
                  // we don't care about errors on this request
                  error: done});
          function done() {
            DAL.cells.currentPoolDetailsCell.invalidate();
          }
        }
      });
    });
  });
};

// setup handling of .disable-if-stale class
(function () {
  function moveAttr(e, fromAttr, toAttr) {
    var toVal = e.getAttribute(toAttr);
    if (!toVal) {
      return;
    }
    var value = e.getAttribute(fromAttr);
    if (!value) {
      return;
    }
    e.removeAttribute(fromAttr);
    e.setAttribute(toAttr, value);
  }
  function processLinks() {
    var enable = !IOCenter.staleness.value;
    $(document.body)[enable ? 'removeClass' : 'addClass']('staleness-active');
    if (enable) {
      $('a.disable-if-stale').each(function (i, e) {
        moveAttr(e, 'disabled-href', 'href');
      });
    } else {
      $('a.disable-if-stale').each(function (i, e) {
        moveAttr(e, 'href', 'disabled-href');
      });
    }
  }
  IOCenter.staleness.subscribeValue(processLinks);
  $(window).bind('template:rendered', processLinks);
})();

$(function () {
  var href = window.location.href;
  var match = /\?(.*?)(?:$|#)/.exec(href);
  if (!match)
    return;
  var params = deserializeQueryString(match[1]);
  if (params['enableInternalSettings']) {
    $('#edit-internal-settings-link').show();
  }
});

(function () {
  var lastCompatMode;

  DAL.cells.runningInCompatMode.subscribeValue(function (value) {
    if (value === undefined) {
      return;
    }
    lastCompatMode = value;
    updateVisibleStuff();
  });

  function updateVisibleStuff() {
    $('.only-when-20')[lastCompatMode ? 'hide' : 'show']();
    $('.only-when-below-20')[lastCompatMode ? 'show' : 'hide']();
  }

  $(window).bind('template:rendered', function () {
    if (lastCompatMode !== undefined) {
      updateVisibleStuff();
    }
  });
})();
