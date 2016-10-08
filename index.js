var coursesFile = 'https://raw.githubusercontent.com/FoothillCollege/StemAnalytics/master/stem_analytics/warehouse/course_records.json?token=AOcnypYf6CWERfmdFNQ69bJMHypkc7EXks5X2vsJwA%3D%3D',
    aboutVisible = false;

$('#time-range').buttonset();

function showDay() {
    $('#day-picker-DEMO').selectmenu('widget').show();
    $('#week-picker').selectmenu('widget').hide();
    $('#quarter-picker').selectmenu('widget').hide();
    setCharts($('#day-picker-DEMO').val());
}

function showWeek() {
    $('#day-picker-DEMO').selectmenu('widget').hide();
    $('#week-picker').selectmenu('widget').show();
    $('#quarter-picker').selectmenu('widget').hide();
    setCharts($('#week-picker').val());
}

function showQuarter() {
    $('#day-picker-DEMO').selectmenu('widget').hide();
    $('#week-picker').selectmenu('widget').hide();
    $('#quarter-picker').selectmenu('widget').show();
    setCharts($('#quarter-picker').val());
}

// $('#datepicker').datepicker();

$('#day-picker-DEMO').selectmenu({
    change: function() { setCharts(this.value); }
});
$('#week-picker').selectmenu({
    change: function() { setCharts(this.value); }
});
$('#quarter-picker').selectmenu({
    change: function() { setCharts(this.value); }
});

var demandData = {
    labels: [],
    datasets: [{
        label: 'Number of Requests',
        backgroundColor: '#48E010',
        data: []
    }]
};

var waitTimeData = {
    labels: [],
    datasets: [{
        label: 'Average Wait Time (Minutes)',
        backgroundColor: '#00A0FF',
        data: []
    }]
};

var demandTrendData = {
    
};

// Populate charts with data
function setCharts(statFile) {
    demandData.labels = [];
    demandData.datasets[0].data = [];
    waitTimeData.labels = [];
    waitTimeData.datasets[0].data = [];
    
    // Wait for JSON request success before using data
    /*$.getJSON(statFile, null, function(data) {
        $.each(data.num_requests, function(label, value) {
            demandData.labels.push(label);
            demandData.datasets[0].data.push(value);
        });
        $.each(data.wait_time, function(label, value) {
            waitTimeData.labels.push(label);
            waitTimeData.datasets[0].data.push(value);
        });
        window.demandChart.update();
        window.waitTimeChart.update();
    })*/
    
    // Use GET request
    $.ajax({
        url: 'https://stem-analytics.herokuapp.com/',
        type: 'get',
        data: {'quarter':'Summer 2015',
               'courses':'all'},
        success: function(data) {
            showError(false);
            $.each(data.num_requests, function(label, value) {
                demandData.labels.push(label);
                demandData.datasets[0].data.push(value);
            });
            $.each(data.wait_time, function(label, value) {
                waitTimeData.labels.push(label);
                waitTimeData.datasets[0].data.push(value);
            });
            /*$.each(data.trend_requests, function(label, value) {
                demandTrendData
            });*/
            $.getJSON(data.course_records, null, function(data2) {
                $.each(data2.ordering, function(index, subject) {
                    var li = $(document.createElement('li')).appendTo('#courseList');
                    $(document.createElement('div')).addClass('arrow').appendTo(li);
                    $(document.createElement('input')).attr({
                        type: 'checkbox',
                        value: subject
                    }).appendTo(li);
                    $(document.createTextNode(subject)).appendTo(li);
                    var ul = $(document.createElement('ul')).appendTo(li);
                    $.each(data2[subject], function(index, course) {
                        li = $(document.createElement('li')).appendTo(ul);
                        $(document.createElement('input')).attr({
                            type: 'checkbox',
                            value: course
                        }).appendTo(li);
                        $(document.createTextNode(course)).appendTo(li);
                    });
                })
                // Make course list expandable
                $('#courseList').find('li:has(ul)')
                    .click( function(event) {
                    if (!$(event.target).is('input')) {
                        $(this).children('.arrow').toggleClass('expanded');
                        $(this).children('ul').toggle('fast');
                    }
                })
                .children('ul').hide();
            })
            window.demandChart.update();
            window.waitTimeChart.update();
            drawHeatmap();
        },
        error: function(xhr) {
            showError(true, xhr);
        }
    });

    resizeHeatmap();
}

function resizeHeatmap() {
    var heatmap = $('#demand-heatmap')
    heatmap.width($('#charts-container').width() * 0.97);
    heatmap.height($('#charts-container').width() * 0.25);
}

function drawHeatmap() {
    
}

function showError(boolShow, xhr) {
    if (boolShow) {
        $('#error').text(xhr);
        $('#error').addClass('shown');
    } else {
        $('#error').removeClass('shown');
    }
}

// Executes after DOM is loaded
$(document).ready(function() {
    // Generate course list
    /*$.getJSON(coursesFile, null, function(data) {
        $.each(data.ordering, function(index, subject) {
            var li = $(document.createElement('li')).appendTo('#courseList');
            $(document.createElement('div')).addClass('arrow').appendTo(li);
            $(document.createElement('input')).attr({
                type: 'checkbox',
                value: subject
            }).appendTo(li);
            $(document.createTextNode(subject)).appendTo(li);
            var ul = $(document.createElement('ul')).appendTo(li);
            $.each(data[subject], function(index, course) {
                li = $(document.createElement('li')).appendTo(ul);
                $(document.createElement('input')).attr({
                    type: 'checkbox',
                    value: course
                }).appendTo(li);
                $(document.createTextNode(course)).appendTo(li);
            });
        })
        // Make course list expandable
        $('#courseList').find('li:has(ul)')
            .click( function(event) {
            if (!$(event.target).is('input')) {
                $(this).children('.arrow').toggleClass('expanded');
                $(this).children('ul').toggle('fast');
            }
        })
        .children('ul').hide();
    })*/
    
    // Render charts
    var demandCtx = document.getElementById('demand-chart').getContext('2d');
    window.demandChart = new Chart(demandCtx, {
        type: 'bar',
        data: demandData,
        options: {
            // Elements options apply to all of the options unless overridden in a dataset
            title: {
                display: true,
                text: 'Tutor Demand'
            },
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Number of Requests'
                    },
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    });
    var waitTimeCtx = document.getElementById('wait-time-chart').getContext('2d');
    window.waitTimeChart = new Chart(waitTimeCtx, {
        type: 'bar',
        data: waitTimeData,
        options: {
            title: {
                display: true,
                text: 'Tutor Wait Time'
            },
            scales: {
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Minutes'
                    },
                    ticks: {
                        beginAtZero: true
                    }
                }]
            }
        }
    });
    
    showQuarter();
});

$(window).on('resize', function() {
    drawHeatmap();
});

$('#about-button').on({
    click: function() {
        if (aboutVisible)
            $('.about').removeClass('visible');
        else
            $('.about').addClass('visible');
        aboutVisible = !aboutVisible;
    }
});
