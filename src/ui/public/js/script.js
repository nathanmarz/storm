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