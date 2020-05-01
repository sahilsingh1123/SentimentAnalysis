/*
 //  Example Code to Execute of use this function...
 var dmxAjax=new DMXAjax();
 dmxAjax.fireAjax(
 {
 type:"POST",     //GET
 data:data,
 url:url,
 serverSuccess:function(response){
 var data=response.data;
 // Use data part of the code.
 },
 serverError:function(response){
 var data=response.data;
 },date
 error:function(response){
 var data=response.data;
 },
 onConnectionRefused:function(){

 },
 onSessionInvalid:function(){

 },
 loading:function(){

 },
 unloading:function(){

 },
 onAbort:function(){

 }
 }
 );
 */


(function (factory) {
    var root = (typeof self == 'object' && self.self === self && self) ||
            (typeof global == 'object' && global.global === global && global);

    root.DMXAjax = factory(root, {}, (root.jQuery || root.$));


})(function (root, DMXAjax, $) {
    var DMXAjax = (function () {
        var ajaxRequest = {};

        var unCheckedSuccess = false;

        function noCheckSucess(val) {
            unCheckedSuccess = val;
        }

        function fireAjax(requestConfiguration) {
            loading();

            var ajaxRequest = {};
            /*var process;
             process.loading();

             if(checkNotNullUndefined(requestConfiguration.loading)){
             process.loading=requestConfiguration.process;
             }

             if(checkNotNullUndefined(requestConfiguration.unloading)){
             process.unloading=requestConfiguration.unloading;
             }*/

            if (checkNotNullUndefined(requestConfiguration.type)) {
                ajaxRequest.type = requestConfiguration.type;
            }

            if (checkNotNullUndefined(requestConfiguration.data)) {
                ajaxRequest.data = requestConfiguration.data;
            }
            if (checkNotNullUndefined(requestConfiguration.url)) {
                ajaxRequest.url = requestConfiguration.url;
            }

            if (checkNotNullUndefined(requestConfiguration.processData)) {
                ajaxRequest.processData = requestConfiguration.processData;
            }
            if (checkNotNullUndefined(requestConfiguration.contentType)) {
                ajaxRequest.contentType = requestConfiguration.contentType;
            }

            if (checkNotNullUndefined(requestConfiguration.async)) {
                ajaxRequest.async = requestConfiguration.async;
            }

            if (checkNotNullUndefined(requestConfiguration.headers)) {
                ajaxRequest.beforeSend = function (xhr) {
                    for (var property in requestConfiguration.headers) {
                        if (requestConfiguration.headers.hasOwnProperty(property)) {
                            xhr.setRequestHeader(property, requestConfiguration.headers[property]);
                        }
                    }
                }
            }

            ajaxRequest.success = function (response) {
                try {
                    if (unCheckedSuccess) {
                        serverSuccess(response);
                    } else if (response.status === 'success') {

                        if (checkNotNullUndefined(response.message)) {
                            DMXErrorBox.success(response.message);
                        }

                        serverSuccess(response);
                    }

                    else if (response.status === 'error') {
                        if (response.SHOW_MESSAGE && checkNotNullUndefined(response.message)) {
                            DMXErrorBox.error(response.message);
                        }
                        serverError(response);
                    }
                    if(response.status==='security'){
                        DMXErrorBox.error(response.data.messages[0]);
                    }
                }
                catch (e) {
                    console.error(e);
                    unloading();
                }
                unloading();
            }

            ajaxRequest.error = function (response) {
                if (checkNotNullUndefined(requestConfiguration.error)) {
                    //  Can't handle the user need to handle this situation.
                    //  Even The message
                    requestConfiguration.error(response);
                }
                unloading();
            }

            $.ajax(ajaxRequest)
                    .fail(
                            function (jqXHR, textStatus, errorThrown) {
                                if (jqXHR.status === 499) {
                                    var pageMessage = new PageMessage();
                                    DMXErrorBox.error(pageMessage.message.sessionInvalid);
                                    fireSessionInvalidCallback();
                                } else if (jqXHR.status === 0) {
                                    fireRequestAborted();
                                } else if (jqXHR.status === 401) {
                                    unAuthenticationError(jqXHR, textStatus, errorThrown);
                                } else if (checkNotNullUndefined(requestConfiguration.error)) {
                                    ajaxRequest.error(jqXHR, textStatus, errorThrown);
                                } else if (jqXHR.status === 500) { // This is wrong, remove me
                                    serverError(jqXHR);
                                }
                                unloading();
                                onFail();
                            }
                    )
                    .then(function (response) {
                        if (requestConfiguration.then && requestConfiguration.then instanceof Function) {
                            try {
                                requestConfiguration.then();
                            } catch (e) {
                                console.error(e);
                            }
                        }
                    });

            function loading() {

                if (checkNotNullUndefined(requestConfiguration.loading)) {
                    requestConfiguration.loading()
                }
            }

            function unloading() {
                try {
                    if (checkNotNullUndefined(requestConfiguration.unloading)) {
                        requestConfiguration.unloading()
                    }
                } catch (e) {
                    PageLoader.hide();
                }
            }

            function onFail() {
                try {
                    if (checkNotNullUndefined(requestConfiguration.onFail)) {
                        requestConfiguration.onFail()
                    }
                } catch (e) {
                    PageLoader.hide();
                }
            }

            function serverSuccess(response) {
                if (checkNotNullUndefined(requestConfiguration.serverSuccess)) {
                    requestConfiguration.serverSuccess(response)
                }
            }

            function fireRequestAborted() {
                if (checkNotNullUndefined(requestConfiguration.onAbort)) {
                    requestConfiguration.onAbort();
                }
            }

            function serverError(response) {
                if (checkNotNullUndefined(requestConfiguration.serverError)) {
                    requestConfiguration.serverError(response)
                }
            }

            function fireConnectionFailedCallback() {
                if (checkNotNullUndefined(requestConfiguration.onConnectionRefused)) {
                    requestConfiguration.onConnectionRefused();
                }
            }

            function fireSessionInvalidCallback() {
                if (checkNotNullUndefined(requestConfiguration.onSessionInvalid)) {
                    requestConfiguration.onSessionInvalid();
                }
            }

            function checkNotNullUndefined(obj) {
                if (obj !== null && obj !== undefined) {
                    return true;
                }
                return false;
            }

            function unAuthenticationError(jqXHR, textStatus, errorThrown) {
                if (checkNotNullUndefined(requestConfiguration.unAuthenticationError)) {
                    requestConfiguration.unAuthenticationError(jqXHR, textStatus, errorThrown);
                }
            }

        }

        function abortAjax() {
            $.ajax(ajaxRequest).abort();
        }

        return {
            noCheckSucess: noCheckSucess,
            fireAjax: fireAjax,
            abortAjax: abortAjax
        }
    });

    return DMXAjax;
});

