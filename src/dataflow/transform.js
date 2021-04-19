function transformCSVtoJSON(line) {
    var values = line.split(',');

    var searched_keyword = {};
    var properties = [
      'user_id',
      'search_keyword',
      'search_result_count',
      'created_at',
    ];
    
    if(values.length > 4){
      searched_keyword[properties[0]] = values[0];
      src_key = ""
      for(var count = 1; count < values.length - 2; count++){
        src_key += values[count]
      }
      searched_keyword[properties[1]] = src_key;
      searched_keyword[properties[2]] = values[count];
      searched_keyword[properties[3]] = values[count+1];
    }else{
      for (var count = 0; count < values.length; count++) {
        if (values[count] !== 'null') {
          searched_keyword[properties[count]] = values[count];
        }
      }
    }
    if(values[2] != "search_result_count"){
      var jsonString = JSON.stringify(searched_keyword);
      return jsonString;
    }
}