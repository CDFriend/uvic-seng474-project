var mymap = L.map('mapid').setView([48.4107174,-123.3423201], 16);

L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoiYWpkZXppZWwiLCJhIjoiY2p0OWx0bmRrMDFsNjQ5bnV2ZnpyNzJrMCJ9.SSwUPVKs_-Y4vJHmPpBPHA', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/">OpenStreetMap</a> contributors, <a href="https://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'your.mapbox.access.token'
}).addTo(mymap);