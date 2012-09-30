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

function toggleSys() {
    var sys = $.cookies.get('sys') || false;
    sys = !sys;

    var exDate = new Date();
    exDate.setDate(exDate.getDate() + 365);

    $.cookies.set('sys', sys, {'path':'/', 'expiresAt':exDate.toUTCString()});
    window.location = window.location;
}

function confirmAction(id, name, action) {
    if (confirm('Do you realy want to ' + action + ' topology ' + name + '?')) {
        window.location.href = '/topology/' + id + '/' + action;
    }
}