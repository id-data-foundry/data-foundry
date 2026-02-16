window.addEventListener('DOMContentLoaded', () => {
  M.AutoInit();

 // initialize special options and fields
  $("select[required]").css({
    display: "inline",
    height: 0,
    padding: 0,
    width: 0
  });
  $('.datepicker').datepicker({format: "yyyy-mm-dd"});
  $('.timepicker').timepicker({default: 'now',twelveHour: false});
  $('.timepicker').on('change', function() {
      var receivedVal = $(this).val();
      $(this).val(receivedVal + ":00");
  });
  $(".countMe").characterCounter();

  if(document.location.hash.length == 0) {
    $('.container form input[type=text]:visible').eq(0).focus();
  }

  // after modal opens, focus on the first visible input element
  $('.modal-trigger').click(function() {
    setTimeout(function() {
      $('.modal.open form input[type=text]:visible').eq(0).focus()
    }, 100);
  });

  // check and init File API support
  (function() {
      if(window.File && window.FileList && window.FileReader) {
          var output = document.getElementById("result");
          var filesInput = document.getElementById("files");
          
          output && filesInput && filesInput.addEventListener("change", function(event) {    
              var files = event.target.files;
              output.innerHTML = "";
              for(var i = 0; i< files.length; i++) {
                  var file = files[i];
                  // only pics
                  if(!file.type.match('image')) {
                    continue;
                  }
                  
                  let fileName = file.name;
                  let lastModified = file.lastModified;
                  let picReader = new FileReader();
                  picReader.addEventListener("load",function(event){ 
                      var picFile = event.target;
                      var div = document.createElement("div");
                      div.innerHTML = "<img class='thumbnail' style='float: left;' src='" + picFile.result + "'" +
                              "title='" + picFile.name + "'/>";
                      output.insertBefore(div,null);

                      $('form').append('<input type="hidden" name="' + fileName + '" value="' + lastModified + '" />');
                  });
                   //Read the image
                  picReader.readAsDataURL(file);
              }
          });
      }
  })();
});

function checkDate() {
  function isBeforeToday(theDate) {
    today = new Date();
    targetDate = new Date(theDate);
    return (today - targetDate) > 0;
  }

  function getToday() {
    today = new Date();
    return today.getFullYear() + "-" + (today.getMonth() <= 8 ? "0" : "") + (today.getMonth() + 1) 
          + "-" + (today.getDate() <= 9 ? "0" : "") + today.getDate();
  }

  var start = $("input#start-date").val(),
      end = $("input#end-date").val();
  
  // end == "", then always set end to be 3 months after start, and always true no matter what start is
  // end != "", check the input format of end to be correct
  if (end == "") {
    return true;
  } else {
    if (end.length != 10 || isNaN(new Date(end).valueOf())) {
      alert("Please input content by correct format and valid date in end Date.");
      return false;
    }
  }

  // if start != "", check the input format of start to be correct
  // end != "" and start == "", if end is < today, make start as end
  // end != "" and start == "", if end is >= today, make start as today
  if (start != "") {
    if (start.length != 10 || isNaN(new Date(start).valueOf())) {
      alert("Please input content by correct format and valid date in Start Date.");
      return false;
    }
  } else {
    if (isBeforeToday(end)) {
      start = end;
    } else {
      start = getToday();
    }
  }

  // checkSequance
  if ((new Date(end)) - (new Date(start)) < 0) {
    alert("Please make sure that the end date after start date.");
    return false;
  }

  return true;
}

function checkDate(db_start, db_end) {
  var start = $("input#start-date").val(),
      end = $("input#end-date").val(),
      startDate,
      endDate;
  
  if (end == "") {
    return true;
  }
  // end != "" 
  if (end != db_end) {
    if (end.length != 10 || isNaN(new Date(end).valueOf())) {
      alert("Please input content by correct format and valid date in end date.");
      return false;
    }
    endDate = new Date(end).valueOf();
  } else {
    // if both start and end are the same as database
    if (start == db_start) {
      return true;
    }
    let dateAry = end.split(" ");
    endDate = new Date(dateAry[5] + "-" + dateAry[1] + "-" + dateAry[2]).valueOf() + 7200000;
  }

  // start != ""
  if (start != "") {
    if (start != db_start) {
      if (start.length != 10 || isNaN(new Date(start).valueOf())) {
        alert("Please input content by correct format and valid date in start date.");
        return false;
      }
      startDate = new Date(start).valueOf();
    } else {
      let dateAry = start.split(" ");
      startDate = new Date(dateAry[5] + "-" + dateAry[1] + "-" + dateAry[2]).valueOf() + 7200000;
    }
  } else {
    // start == ""
    if (new Date() - endDate > 0) {
      startDate = endDate;
    } else {
      startDate = new Date().valueOf();
    }
  }

  // checkSequance
  if (endDate - startDate < 0) {
    alert("Please make sure that the end date is after the start date.");
    return false;
  }

  return true;
}

function switchModal(elementID, operation) {
  $(elementID).modal(operation);
}
