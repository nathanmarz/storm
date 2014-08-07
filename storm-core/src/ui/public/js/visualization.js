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

// Inspired by
// https://github.com/samizdatco/arbor/blob/master/docs/sample-project/main.js

function renderGraph(elem) {

    var canvas = $(elem).get(0);
    canvas.width = $(window).width();
    canvas.height = $(window).height();
    var ctx = canvas.getContext("2d");
    var gfx = arbor.Graphics(canvas);
    var psys;

    var totaltrans = 0;
    var weights = {};
    var texts = {};
    var update = false;

    var myRenderer = {
        init: function(system){ 
            psys = system;
            psys.screenSize(canvas.width, canvas.height)
            psys.screenPadding(20);
            myRenderer.initMouseHandling();
        },

        signal_update: function() {
            update = true;
        },

        redraw: function() { 
            
            if(!psys)
                return;
            
            if(update) {
                totaltrans = calculate_total_transmitted(psys);
                weights = calculate_weights(psys, totaltrans);
                texts = calculate_texts(psys, totaltrans);
                update = false;
            }



            ctx.fillStyle = "white";
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            var x = 0;
            

            psys.eachEdge(function(edge, pt1, pt2) {

                var len = Math.sqrt(Math.pow(pt2.x - pt1.x,2) + Math.pow(pt2.y - pt1.y,2));
                var sublen = len - (Math.max(50, 20 + gfx.textWidth(edge.target.name)) / 2);
                var thirdlen = len/3;
                var theta = Math.atan2(pt2.y - pt1.y, pt2.x - pt1.x);
                
                var newpt2 = {
                    x : pt1.x + (Math.cos(theta) * sublen),
                    y : pt1.y + (Math.sin(theta) * sublen)
                };

                var thirdpt = {
                    x: pt1.x + (Math.cos(theta) * thirdlen),
                    y: pt1.y + (Math.sin(theta) * thirdlen)
                }

                weight = weights[edge.source.name + edge.target.name];
                
                if(!weights[edge.source.name + edge.target.name])
                {
                    totaltrans = calculate_total_transmitted(psys);
                    weights = calculate_weights(psys, totaltrans);
                }

                ctx.strokeStyle = "rgba(0,0,0, .333)";
                ctx.lineWidth = 25 * weight + 5;
                ctx.beginPath();

                var arrlen = 15;
                ctx.moveTo(pt1.x, pt1.y);
                ctx.lineTo(newpt2.x, newpt2.y);
                ctx.lineTo(newpt2.x - arrlen * Math.cos(theta-Math.PI/6), newpt2.y - arrlen * Math.sin(theta - Math.PI/6));
                ctx.moveTo(newpt2.x, newpt2.y);
                ctx.lineTo(newpt2.x - arrlen * Math.cos(theta+Math.PI/6), newpt2.y - arrlen * Math.sin(theta + Math.PI/6));

                
                if (texts[edge.source.name + edge.target.name] == null)
                {
                    totaltrans = calculate_total_transmitted(psys);
                    texts = calculate_texts(psys, totaltrans);
                }

                gfx.text(texts[edge.source.name + edge.target.name], thirdpt.x, thirdpt.y + 10, {color:"black", align:"center", font:"Arial", size:10})
                ctx.stroke();
            });

            psys.eachNode(function(node, pt) {
                var col;

                var real_trans = gather_stream_count(node.data[":stats"], "default", "600");
                
                if(node.data[":type"] === "bolt") {
                    var cap = Math.min(node.data[":capacity"], 1);
                    var red = Math.floor(cap * 225) + 30;
                    var green = Math.floor(255 - red);
                    var blue = Math.floor(green/5);
                    col = arbor.colors.encode({r:red,g:green,b:blue,a:1});
                } else {
                    col = "#0000FF";
                }
                
                var w = Math.max(55, 25 + gfx.textWidth(node.name));
                
                gfx.oval(pt.x - w/2, pt.y - w/2, w, w, {fill: col});
                gfx.text(node.name, pt.x, pt.y+3, {color:"white", align:"center", font:"Arial", size:12});
                gfx.text(node.name, pt.x, pt.y+3, {color:"white", align:"center", font:"Arial", size:12});
                
                gfx.text(parseFloat(node.data[":latency"]).toFixed(2) + " ms", pt.x, pt.y + 17, {color:"white", align:"center", font:"Arial", size:12});
                
            });

            // Draw gradient sidebar
            ctx.rect(0,0,50,canvas.height);
            var grd = ctx.createLinearGradient(0,0,50,canvas.height);
            grd.addColorStop(0, '#1ee12d');
            grd.addColorStop(1, '#ff0000');
            ctx.fillStyle=grd;
            ctx.fillRect(0,0,50,canvas.height);
            
            
        },
        
        initMouseHandling:function() {
            var dragged = null;

            var clicked = false;
            
            var handler = {
                clicked:function(e){
                    var pos = $(canvas).offset();
                    _mouseP = arbor.Point(e.pageX-pos.left, e.pageY - pos.top);
                    dragged = psys.nearest(_mouseP);
                    
                    if(dragged && dragged.node !== null) {
                        dragged.node.fixed = true;
                    }
                    
                    clicked = true;
                    setTimeout(function(){clicked = false;}, 50);

                    $(canvas).bind('mousemove', handler.dragged);
                    $(window).bind('mouseup', handler.dropped);
                    
                    return false;
                },
                
                dragged:function(e) {

                    var pos = $(canvas).offset();
                    var s = arbor.Point(e.pageX-pos.left, e.pageY-pos.top);
                    
                    if(dragged && dragged.node != null) {
                        var p = psys.fromScreen(s);
                        dragged.node.p = p;
                    }
                    
                    return false;
                    
                },

                dropped:function(e) {
                    if(clicked) {
                        if(dragged.distance < 50) {
                            if(dragged && dragged.node != null) { 
                                window.location = dragged.node.data[":link"];
                            }
                        }
                    }

                    if(dragged === null || dragged.node === undefined) return;
                    if(dragged.node !== null) dragged.node.fixed = false;
                    dragged.node.tempMass = 1000;
                    dragged = null;
                    $(canvas).unbind('mousemove', handler.dragged);
                    $(window).unbind('mouseup', handler.dropped);
                    _mouseP = null;
                    return false;
                }
                
            }
            
            $(canvas).mousedown(handler.clicked);
        }
    }
    
    return myRenderer;
}

function calculate_texts(psys, totaltrans) {
    var texts = {};
    psys.eachEdge(function(edge, pt1, pt2) {
        var text = "";
        for(var i = 0; i < edge.target.data[":inputs"].length; i++) {
            var stream = edge.target.data[":inputs"][i][":stream"];
            var sani_stream = edge.target.data[":inputs"][i][":sani-stream"];
            if(stream_checked(sani_stream) 
               && edge.target.data[":inputs"][i][":component"] === edge.source.name) {
                stream_transfered = gather_stream_count(edge.source.data[":stats"], sani_stream, "600");
                text += stream + ": " 
                    + stream_transfered + ": " 
                    + (totaltrans > 0  ? Math.round((stream_transfered/totaltrans) * 100) : 0) + "%\n";
                
            }
        }
        
        texts[edge.source.name + edge.target.name] = text;
    });

    return texts;
}

function calculate_weights(psys, totaltrans) {
    var weights = {};
 
    psys.eachEdge(function(edge, pt1, pt2) {
        var trans = 0;
        for(var i = 0; i < edge.target.data[":inputs"].length; i++) {
            var stream = edge.target.data[":inputs"][i][":sani-stream"];
            if(stream_checked(stream) && edge.target.data[":inputs"][i][":component"] === edge.source.name)
                trans += gather_stream_count(edge.source.data[":stats"], stream, "600");
        }
        weights[edge.source.name + edge.target.name] = (totaltrans > 0 ? trans/totaltrans : 0);
    });
    return weights;
}

function calculate_total_transmitted(psys) {
    var totaltrans = 0;
    var countedmap = {}
    psys.eachEdge(function(node, pt, pt2) {
        if(!countedmap[node.source.name])
            countedmap[node.source.name] = {};

        for(var i = 0; i < node.target.data[":inputs"].length; i++) {
            var stream = node.target.data[":inputs"][i][":stream"];
            if(stream_checked(node.target.data[":inputs"][i][":sani-stream"]))
            {
                if(!countedmap[node.source.name][stream]) {
                    if(node.source.data[":stats"])
                    {
                        var toadd = gather_stream_count(node.source.data[":stats"], node.target.data[":inputs"][i][":sani-stream"], "600");
                        totaltrans += toadd;
                    }
                    countedmap[node.source.name][stream] = true;
                }
            }
        }
        
    });

    return totaltrans;
}

function has_checked_stream_input(inputs) {
    
    for(var i = 0; i < inputs.length; i++) {
        var x = stream_checked(inputs[i][":sani-stream"]);
        if(x) 
            return true;
    }
    return false;
}

function stream_checked(stream) {
    var checked = $("#" + stream).is(":checked");
    return checked;
}

function has_checked_stream_output(jdat, component) {
    var ret = false;
    $.each(jdat, function(k, v) {
        for(var i = 0; i < v[":inputs"].length; i++) {
            if(stream_checked(v[":inputs"][i][":sani-stream"]) 
               && v[":inputs"][i][":component"] == component)
                ret = true;
        }
    });
    return ret;
}

function gather_stream_count(stats, stream, time) {
    var transferred = 0;
    if(stats)
        for(var i = 0; i < stats.length; i++) {
            if(stats[i][":transferred"] != null)
            {
                var stream_trans = stats[i][":transferred"][time][stream];
                if(stream_trans != null)
                    transferred += stream_trans;
            }
        }
    return transferred;
}


function rechoose(jdat, sys, box) {
    var id = box.id;
    if($(box).is(':checked'))
    {
        //Check each node in our json data to see if it has inputs from or outputs to selected streams. If it does, add a node for it.
        $.each(jdat,function(k,v) {
            if( has_checked_stream_input(v[":inputs"]) || has_checked_stream_output(jdat, k))
                sys.addNode(k,v);
        });
           
        //Check each node in our json data and add necessary edges based on selected components.
        $.each(jdat, function(k, v) {
            for(var i = 0; i < v[":inputs"].length; i++)
                if(v[":inputs"][i][":sani-stream"] === id) {
                    
                    sys.addEdge(v[":inputs"][i][":component"], k, v);
                }
        });
    }
    else {
        //Check each node to see if it should be pruned.
        sys.prune(function(node, from, to) {
            return !has_checked_stream_input(node.data[":inputs"]) && !has_checked_stream_output(jdat, node.name);
        });
        
        //Check each edge to see if it represents any selected streams. If not, prune it.
        sys.eachEdge(function(edge, pt1, pt2) {
            var inputs = edge.target.data[":inputs"];
            
            if($.grep(inputs, function(input) {
                
                return input[":component"] === edge.source.name 
                    && stream_checked(input[":sani-stream"]);
            
            }).length == 0)
            {
                sys.pruneEdge(edge);
            }
        });
    }

    //Tell the particle system's renderer that it needs to update its labels, colors, widths, etc.
    sys.renderer.signal_update();
    sys.renderer.redraw();

}

var topology_data;
function update_data(jdat, sys) {
    $.each(jdat, function(k,v) {
        if(sys.getNode(k))
            sys.getNode(k).data = v;
    });
}

var should_update;
function show_visualization(sys) {

    if(sys == null)
    {
        sys = arbor.ParticleSystem(20, 1000, 0.15, true, 55, 0.02, 0.6);
        sys.renderer = renderGraph("#topoGraph");
        sys.stop();

        $(".stream-box").click(function () { rechoose(topology_data, sys, this) });    
    }

    should_update = true;
    var update_freq_ms = 10000;
    var update = function(should_rechoose){
        $.ajax({
            url: "/api/v1/topology/"+$.url().param("id")+"/visualization",
            success: function(data, status, jqXHR) {
                topology_data = data;
                update_data(topology_data, sys);
                sys.renderer.signal_update();
                sys.renderer.redraw();
                if(should_update)
                    setTimeout(update, update_freq_ms);
                if(should_rechoose)
                    $(".stream-box").each(function () { rechoose(topology_data, sys, this) });
            }
        });
    };
    
    update(true);
    $("#visualization-container").show(500);
    $("#show-hide-visualization").attr('value', 'Hide Visualization');
    $("#show-hide-visualization").unbind("click");
    $("#show-hide-visualization").click(function () { hide_visualization(sys) });
}

function hide_visualization(sys) {
    should_update = false;
    $("#visualization-container").hide(500);
    $("#show-hide-visualization").attr('value', 'Show Visualization');
    $("#show-hide-visualization").unbind("click");
    $("#show-hide-visualization").click(function () { show_visualization(sys) });
}
