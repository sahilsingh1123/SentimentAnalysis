var sentimentVar = '';

var Analyze = $("#SAAnalyze");
Analyze.on("click", function () {
    sentimentVar = $('#text_area').val();
    //loadPage('index.html') //--> will get back to it later..
    //this is for vivken sentiment only... as we add more diversity we will
    //call different method accordingly
    getViveknSentiment();
});


function getViveknSentiment() {
    $.ajax({
        type: 'POST',
        url: '/sentimentResult/viveknSentiment',
        data:{
            sentimentText: sentimentVar
        },
        success: function (response) {
            var result = response.result;
            var message = response.message;
            document.getElementById('sentimentResultDisplay').value = result;
        }
    });

}

//this method is not used for now but we will think of extending and making some
//great use of this method too....
function loadPage(path) {
    var response = null;
    var xhr = new XMLHttpRequest();
    xhr.open("GET", path, false);
    xhr.onload = function (e) {
        if (xhr.readyState === 4) {
            if (xhr.status === 200) {
                response = xhr.responseText;
            } else {
                console.error(xhr.statusText)
            }
        }
    };
    xhr.onerror = function (e) {
        console.error(xhr.statusText);
    };
    xhr.send();
    return response;
}