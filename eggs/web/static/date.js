String.prototype.toDate = function(str) {
    var m = this.match(/(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)/);
    return new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
}

Date.prototype.addDays = function(d){
    this.setDate(this.getDate()+d);
    return this;
}

Date.prototype.addHours = function(h){
    this.setHours(this.getHours()+h);
    return this;
}

function pad ( num, size ) {
  if (num.toString().length >= size) return num;
  return (Math.pow( 10, size) + Math.floor(num)).toString().substring(1);
}        

Date.prototype.toString = function() {
    return pad(this.getFullYear(), 4) + '-' + pad(this.getMonth()+1, 2)+ '-' + pad(this.getDate(), 2)+ ' '+ pad(this.getHours(), 2) + ':' + pad(this.getMinutes(), 2) + ':' + pad(this.getSeconds(), 2);
}
