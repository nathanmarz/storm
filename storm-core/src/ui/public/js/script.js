/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
$.tablesorter.addParser({
    id:'stormtimestr',
    is:function (s) {
        return false;
    },
    format:function (s) {
        if (s.search('All time') != -1) {
            return 1000000000;
        }
        var total = 0;
        $.each(s.split(' '), function (i, v) {
            var amt = parseInt(v);
            if (v.search('ms') != -1) {
                total += amt;
            } else if (v.search('s') != -1) {
                total += amt * 1000;
            } else if (v.search('m') != -1) {
                total += amt * 1000 * 60;
            } else if (v.search('h') != -1) {
                total += amt * 1000 * 60 * 60;
            } else if (v.search('d') != -1) {
                total += amt * 1000 * 60 * 60 * 24;
            }
        });
        return total;
    },
    type:'numeric'
});

$(function () {
    $(".js-only").show();
});

function toggleSys() {
    var sys = $.cookies.get('sys') || false;
    sys = !sys;

    var exDate = new Date();
    exDate.setDate(exDate.getDate() + 365);

    $.cookies.set('sys', sys, {'path':'/', 'expiresAt':exDate.toUTCString()});
    window.location = window.location;
}

function ensureInt(n) {
    var isInt = /^\d+$/.test(n);
    if (!isInt) {
        alert("'" + n + "' is not integer.");
    }

    return isInt;
}

function confirmAction(id, name, action, wait, defaultWait) {
    var opts = {
        type:'POST',
        url:'/topology/' + id + '/' + action
    };
    if (wait) {
        var waitSecs = prompt('Do you really want to ' + action + ' topology "' + name + '"? ' +
                              'If yes, please, specify wait time in seconds:',
                              defaultWait);

        if (waitSecs != null && waitSecs != "" && ensureInt(waitSecs)) {
            opts.url += '/' + waitSecs;
        } else {
            return false;
        }
    } else if (!confirm('Do you really want to ' + action + ' topology "' + name + '"?')) {
        return false;
    }

    $("input[type=button]").attr("disabled", "disabled");
    $.ajax(opts).always(function () {
        window.location.reload();
    }).fail(function () {
        alert("Error while communicating with Nimbus.")
    });

    return false;
}

$(function () {
    var placements = ['above', 'below', 'left', 'right'];
    for (var i in placements) {
      $('.tip.'+placements[i]).twipsy({
          live: true,
          placement: placements[i],
          delayIn: 1000
      });
    }
})
