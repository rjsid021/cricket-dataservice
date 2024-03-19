import pandas as pd

from DataIngestion.query import GET_PLAYER_MAPPER_SQL
from DataIngestion.utils.helper import readCSV
from common.dao.fetch_db_data import getPandasFactoryDF
from common.dao_client import session

WPL = [
    "https://www.espncricinfo.com/cricketers/meg-lanning-329336",
    "https://www.espncricinfo.com/cricketers/taniya-bhatia-883423",
    "https://www.espncricinfo.com/cricketers/laura-harris-951163",
    "https://www.espncricinfo.com/cricketers/jemimah-rodrigues-883405",
    "https://www.espncricinfo.com/cricketers/shafali-verma-1182523",
    "https://www.espncricinfo.com/cricketers/alice-capsey-1187120",
    "https://www.espncricinfo.com/cricketers/marizanne-kapp-351836",
    "https://www.espncricinfo.com/cricketers/shikha-pandey-442145",
    "https://www.espncricinfo.com/cricketers/jess-jonassen-374936",
    "https://www.espncricinfo.com/cricketers/minnu-mani-960949",
    "https://www.espncricinfo.com/cricketers/tara-norris-872217",
    "https://www.espncricinfo.com/cricketers/poonam-yadav-630972",
    "https://www.espncricinfo.com/cricketers/arundhati-reddy-960867",
    "https://www.espncricinfo.com/cricketers/titas-sadhu-1255407",
    "https://www.espncricinfo.com/cricketers/radha-yadav-960737",
    "https://www.espncricinfo.com/cricketers/jasia-akhtar-961035",
    "https://www.espncricinfo.com/cricketers/aparna-mondal-960745",
    "https://www.espncricinfo.com/cricketers/sneha-deepthi-627072",

    "https://www.espncricinfo.com/cricketers/harleen-deol-960845",
    "https://www.espncricinfo.com/cricketers/dayalan-hemalatha-961107",
    "https://www.espncricinfo.com/cricketers/sabbhineni-meghana-556529",
    "https://www.espncricinfo.com/cricketers/sushma-verma-597821",
    "https://www.espncricinfo.com/cricketers/laura-wolvaardt-922481",
    "https://www.espncricinfo.com/cricketers/beth-mooney-381258",
    "https://www.espncricinfo.com/cricketers/sneh-rana-556537",
    "https://www.espncricinfo.com/cricketers/sophia-dunkley-885815",
    "https://www.espncricinfo.com/cricketers/hurley-gala-1289680",
    "https://www.espncricinfo.com/cricketers/ashleigh-gardner-858809",
    "https://www.espncricinfo.com/cricketers/kim-garth-418423",
    "https://www.espncricinfo.com/cricketers/annabel-sutherland-1071705",
    "https://www.espncricinfo.com/cricketers/deandra-dottin-355349",
    "https://www.espncricinfo.com/cricketers/mansi-joshi-960815",
    "https://www.espncricinfo.com/cricketers/tanuja-kanwar-960847",
    "https://www.espncricinfo.com/cricketers/shabnam-md-1346683",
    "https://www.espncricinfo.com/cricketers/georgia-wareham-946057",
    "https://www.espncricinfo.com/cricketers/ashwani-kumari-1345792",
    "https://www.espncricinfo.com/cricketers/monica-patel-1213438",
    "https://www.espncricinfo.com/cricketers/parunika-sisodia-1289559",

    "https://www.espncricinfo.com/cricketers/yastika-bhatia-960715",
    "https://www.espncricinfo.com/cricketers/hayley-matthews-474308",
    "https://www.espncricinfo.com/cricketers/harmanpreet-kaur-372317",
    "https://www.espncricinfo.com/cricketers/heather-graham-546629",
    "https://www.espncricinfo.com/cricketers/amanjot-kaur-1255411",
    "https://www.espncricinfo.com/cricketers/amelia-kerr-803971",
    "https://www.espncricinfo.com/cricketers/nat-sciver-brunt-515905",
    "https://www.espncricinfo.com/cricketers/chloe-tryon-453370",
    "https://www.espncricinfo.com/cricketers/saika-ishaque-597815",
    "https://www.espncricinfo.com/cricketers/jintimani-kalita-1255387",
    "https://www.espncricinfo.com/cricketers/pooja-vastrakar-883413",
    "https://www.espncricinfo.com/cricketers/issy-wong-1146066",
    "https://www.espncricinfo.com/cricketers/sonam-yadav-1289956",
    "https://www.espncricinfo.com/cricketers/priyanka-bala-1255564",
    "https://www.espncricinfo.com/cricketers/neelam-bisht-961029",
    "https://www.espncricinfo.com/cricketers/dhara-gujjar-1255408",
    "https://www.espncricinfo.com/cricketers/humaira-kazi-961005",

    "https://www.espncricinfo.com/cricketers/smriti-mandhana-597806",
    "https://www.espncricinfo.com/cricketers/richa-ghosh-1212830",
    "https://www.espncricinfo.com/cricketers/disha-kasat-961183",
    "https://www.espncricinfo.com/cricketers/heather-knight-358259",
    "https://www.espncricinfo.com/cricketers/indrani-roy-1255494",
    "https://www.espncricinfo.com/cricketers/kanika-ahuja-1255560",
    "https://www.espncricinfo.com/cricketers/sobhana-asha-550687",
    "https://www.espncricinfo.com/cricketers/erin-burns-275447",
    "https://www.espncricinfo.com/cricketers/sophie-devine-231740",
    "https://www.espncricinfo.com/cricketers/shreyanka-patil-1289948",
    "https://www.espncricinfo.com/cricketers/ellyse-perry-275487",
    "https://www.espncricinfo.com/cricketers/dane-van-niekerk-364413",
    "https://www.espncricinfo.com/cricketers/sahana-pawar-960931",
    "https://www.espncricinfo.com/cricketers/preeti-bose-709843",
    "https://www.espncricinfo.com/cricketers/renuka-singh-960853",
    "https://www.espncricinfo.com/cricketers/megan-schutt-420314",
    "https://www.espncricinfo.com/cricketers/komal-zanzad-961195",
    "https://www.espncricinfo.com/cricketers/poonam-khemnar-1289979",

    "https://www.espncricinfo.com/cricketers/alyssa-healy-275486",
    "https://www.espncricinfo.com/cricketers/kiran-navgire-1289983",
    "https://www.espncricinfo.com/cricketers/simran-shaikh-1255541",
    "https://www.espncricinfo.com/cricketers/alyssa-healy-275486",
    "https://www.espncricinfo.com/cricketers/kiran-navgire-1289983",
    "https://www.espncricinfo.com/cricketers/simran-shaikh-1255541",
    "https://www.espncricinfo.com/cricketers/parshavi-chopra-1346678",
    "https://www.espncricinfo.com/cricketers/grace-harris-381268",
    "https://www.espncricinfo.com/cricketers/tahlia-mcgrath-381311",
    "https://www.espncricinfo.com/cricketers/shweta-sehrawat-1255442",
    "https://www.espncricinfo.com/cricketers/deepti-sharma-597811",
    "https://www.espncricinfo.com/cricketers/devika-vaidya-709837",
    "https://www.espncricinfo.com/cricketers/anjali-sarvani-960673",
    "https://www.espncricinfo.com/cricketers/lauren-bell-878025",
    "https://www.espncricinfo.com/cricketers/sophie-ecclestone-878039",
    "https://www.espncricinfo.com/cricketers/rajeshwari-gayakwad-709635",
    "https://www.espncricinfo.com/cricketers/shabnim-ismail-276997",
    "https://www.espncricinfo.com/cricketers/shivali-shinde-960993",
    "https://www.espncricinfo.com/cricketers/soppadhandi-yashasri-1255485",
    "https://www.espncricinfo.com/cricketers/laxmi-yadav-1289558",
]

ILT20 = [
    "https://www.espncricinfo.com/cricketers/connor-esterhuizen-1070997",
    "https://www.espncricinfo.com/cricketers/colin-ingram-45705",
    "https://www.espncricinfo.com/cricketers/brandon-king-670035",
    "https://www.espncricinfo.com/cricketers/kennar-lewis-537319",
    "https://www.espncricinfo.com/cricketers/sunil-narine-230558",
    "https://www.espncricinfo.com/cricketers/charith-asalanka-784367",
    "https://www.espncricinfo.com/cricketers/dhananjaya-de-silva-465793",
    "https://www.espncricinfo.com/cricketers/fahad-nawaz-1126002",
    "https://www.espncricinfo.com/cricketers/raymon-reifer-450101",
    "https://www.espncricinfo.com/cricketers/andre-russell-276298",
    "https://www.espncricinfo.com/cricketers/paul-stirling-303427",
    "https://www.espncricinfo.com/cricketers/ali-khan-927119",
    "https://www.espncricinfo.com/cricketers/marchant-de-lange-393279",
    "https://www.espncricinfo.com/cricketers/akeal-hosein-530812",
    "https://www.espncricinfo.com/cricketers/brandon-glover-595419",
    "https://www.espncricinfo.com/cricketers/lahiru-kumara-784375",
    "https://www.espncricinfo.com/cricketers/treveen-mathew-1282477",
    "https://www.espncricinfo.com/cricketers/matiullah-khan-1241201",
    "https://www.espncricinfo.com/cricketers/ravi-rampaul-52912",
    "https://www.espncricinfo.com/cricketers/sabir-ali-1328438",
    "https://www.espncricinfo.com/cricketers/zawar-farid-1196500",

    "https://www.espncricinfo.com/cricketers/colin-munro-232359",
    "https://www.espncricinfo.com/cricketers/sam-billings-297628",
    "https://www.espncricinfo.com/cricketers/dinesh-chandimal-300628",
    "https://www.espncricinfo.com/cricketers/alex-hales-249866",
    "https://www.espncricinfo.com/cricketers/adam-lyth-251721",
    "https://www.espncricinfo.com/cricketers/rohan-mustafa-307808",
    "https://www.espncricinfo.com/cricketers/sherfane-rutherford-914541",
    "https://www.espncricinfo.com/cricketers/ali-naseer-1212829",
    "https://www.espncricinfo.com/cricketers/tom-curran-550235",
    "https://www.espncricinfo.com/cricketers/wanindu-hasaranga-784379",
    "https://www.espncricinfo.com/cricketers/benny-howell-211748",
    "https://www.espncricinfo.com/cricketers/ronak-panoly-1161037",
    "https://www.espncricinfo.com/cricketers/gus-atkinson-1039481",
    "https://www.espncricinfo.com/cricketers/sheldon-cottrell-495551",
    "https://www.espncricinfo.com/cricketers/jake-lintott-463668",
    "https://www.espncricinfo.com/cricketers/tymal-mills-459257",
    "https://www.espncricinfo.com/cricketers/matheesha-pathirana-1194795",
    "https://www.espncricinfo.com/cricketers/shiraz-ahmed-1206625",
    "https://www.espncricinfo.com/cricketers/ruben-trumpelmann-698317",
    "https://www.espncricinfo.com/cricketers/mark-watt-659081",

    "https://www.espncricinfo.com/cricketers/rovman-powell-820351",
    "https://www.espncricinfo.com/cricketers/chirag-suri-534734",
    "https://www.espncricinfo.com/cricketers/niroshan-dickwella-429754",
    "https://www.espncricinfo.com/cricketers/hazratullah-zazai-793457",
    "https://www.espncricinfo.com/cricketers/dan-lawrence-641423",
    "https://www.espncricinfo.com/cricketers/george-munsey-671805",
    "https://www.espncricinfo.com/cricketers/bhanuka-rajapaksa-342619",
    "https://www.espncricinfo.com/cricketers/joe-root-303669",
    "https://www.espncricinfo.com/cricketers/robin-uthappa-35582",
    "https://www.espncricinfo.com/cricketers/fabian-allen-670013",
    "https://www.espncricinfo.com/cricketers/ravi-bopara-10582",
    "https://www.espncricinfo.com/cricketers/chamika-karunaratne-623695",
    "https://www.espncricinfo.com/cricketers/yusuf-pathan-32498",
    "https://www.espncricinfo.com/cricketers/dasun-shanaka-437316",
    "https://www.espncricinfo.com/cricketers/sikandar-raza-299572",
    "https://www.espncricinfo.com/cricketers/isuru-udana-328026",
    "https://www.espncricinfo.com/cricketers/akif-raja-681067",
    "https://www.espncricinfo.com/cricketers/jash-giyanani-1294146",
    "https://www.espncricinfo.com/cricketers/hazrat-luqman-1241269",
    "https://www.espncricinfo.com/cricketers/fred-klaassen-1104703",
    "https://www.espncricinfo.com/cricketers/mujeeb-ur-rahman-974109",
    "https://www.espncricinfo.com/cricketers/ollie-white-1193529",

    "https://www.espncricinfo.com/cricketers/james-vince-296597",
    "https://www.espncricinfo.com/cricketers/tom-banton-877051",
    "https://www.espncricinfo.com/cricketers/shimron-hetmyer-670025",
    "https://www.espncricinfo.com/cricketers/chris-lynn-326637",
    "https://www.espncricinfo.com/cricketers/ollie-pope-887207",
    "https://www.espncricinfo.com/cricketers/chundangapoyil-rizwan-474760",
    "https://www.espncricinfo.com/cricketers/ashwanth-valthapa-1199669",
    "https://www.espncricinfo.com/cricketers/aayan-afzal-khan-1241265",
    "https://www.espncricinfo.com/cricketers/carlos-brathwaite-457249",
    "https://www.espncricinfo.com/cricketers/liam-dawson-211855",
    "https://www.espncricinfo.com/cricketers/dominic-drakes-906749",
    "https://www.espncricinfo.com/cricketers/gerhard-erasmus-519070",
    "https://www.espncricinfo.com/cricketers/rehan-ahmed-1263691",
    "https://www.espncricinfo.com/cricketers/david-wiese-221140",
    "https://www.espncricinfo.com/cricketers/jamie-overton-510530",
    "https://www.espncricinfo.com/cricketers/richard-gleeson-473191",
    "https://www.espncricinfo.com/cricketers/tom-helm-512907",
    "https://www.espncricinfo.com/cricketers/chris-jordan-288992",
    "https://www.espncricinfo.com/cricketers/qais-ahmad-914171",
    "https://www.espncricinfo.com/cricketers/sanchit-sharma-1199670",

    "https://www.espncricinfo.com/cricketers/vriitya-aravind-1178586",
    "https://www.espncricinfo.com/cricketers/basil-hameed-1209110",
    "https://www.espncricinfo.com/cricketers/andre-fletcher-51862",
    "https://www.espncricinfo.com/cricketers/muhammad-waseem-1241277",
    "https://www.espncricinfo.com/cricketers/najibullah-zadran-524049",
    "https://www.espncricinfo.com/cricketers/nicholas-pooran-604302",
    "https://www.espncricinfo.com/cricketers/will-smeed-1099224",
    "https://www.espncricinfo.com/cricketers/lorcan-tucker-928057",
    "https://www.espncricinfo.com/cricketers/kieron-pollard-230559",
    "https://www.espncricinfo.com/cricketers/dwayne-bravo-51439",
    "https://www.espncricinfo.com/cricketers/bas-de-leede-1036191",
    "https://www.espncricinfo.com/cricketers/tom-lammonby-902907",
    "https://www.espncricinfo.com/cricketers/dan-mousley-1172968",
    "https://www.espncricinfo.com/cricketers/craig-overton-464626",
    "https://www.espncricinfo.com/cricketers/samit-patel-18632",
    "https://www.espncricinfo.com/cricketers/jordan-thompson-766809",
    "https://www.espncricinfo.com/cricketers/trent-boult-277912",
    "https://www.espncricinfo.com/cricketers/mckenny-clarke-1275937",
    "https://www.espncricinfo.com/cricketers/fazalhaq-farooqi-974175",
    "https://www.espncricinfo.com/cricketers/imran-tahir-40618",
    "https://www.espncricinfo.com/cricketers/brad-wheal-807535",
    "https://www.espncricinfo.com/cricketers/zahir-khan-712219",
    "https://www.espncricinfo.com/cricketers/zahoor-khan-384525",

    "https://www.espncricinfo.com/cricketers/chris-benjamin-1149157",
    "https://www.espncricinfo.com/cricketers/joe-denly-12454",
    "https://www.espncricinfo.com/cricketers/mark-deyal-848623",
    "https://www.espncricinfo.com/cricketers/tom-kohler-cadmore-470633",
    "https://www.espncricinfo.com/cricketers/evin-lewis-431901",
    "https://www.espncricinfo.com/cricketers/dawid-malan-236489",
    "https://www.espncricinfo.com/cricketers/rahmanullah-gurbaz-974087",
    "https://www.espncricinfo.com/cricketers/alishan-sharafu-1161038",
    "https://www.espncricinfo.com/cricketers/moeen-ali-8917",
    "https://www.espncricinfo.com/cricketers/mohammad-nabi-25913",
    "https://www.espncricinfo.com/cricketers/marcus-stoinis-325012",
    "https://www.espncricinfo.com/cricketers/paul-walter-909225",
    "https://www.espncricinfo.com/cricketers/chris-woakes-247235",
]

MLC = [
    "/cricketers/unmukt-chand-446499",
    "/cricketers/martin-guptill-226492",
    "/cricketers/nitish-kumar-348129",
    "/cricketers/jaskaran-malhotra-430105",
    "/cricketers/rilee-rossouw-318845",
    "/cricketers/jason-roy-298438",
    "/cricketers/saif-badar-922967",
    "/cricketers/gajanand-singh-230551",
    "/cricketers/sunil-narine-230558",
    "/cricketers/corn-dry-429927",
    "/cricketers/andre-russell-276298",
    "/cricketers/shadley-van-schalkwyk-334621",
    "/cricketers/bhaskar-yadram-1078699",
    "/cricketers/ali-khan-927119",
    "/cricketers/ali-sheikh-1193309",
    "/cricketers/lockie-ferguson-493773",
    "/cricketers/spencer-johnson-1123718",
    "/cricketers/adam-zampa-379504",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757",

    "/cricketers/tim-david-892749",
    "/cricketers/hammad-azam-384518",
    "/cricketers/monank-patel-1159641",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/shayan-jahangir-647771",
    "/cricketers/tristan-stubbs-595978",
    "/cricketers/steven-taylor-348133",
    "/cricketers/saideep-ganesh-1345498",
    "/cricketers/kieron-pollard-230559",
    "/cricketers/dewald-brevis-1070665",
    "/cricketers/rashid-khan-793463",
    "/cricketers/david-wiese-221140",
    "/cricketers/jason-behrendorff-272477",
    "/cricketers/trent-boult-277912",
    "/cricketers/ehsan-adil-547092",
    "/cricketers/jessy-singh-772471",
    "/cricketers/nosthush-kenjige-1041679",
    "/cricketers/sarabjit-ladda-317292",
    "/cricketers/kagiso-rabada-550215",
    "/cricketers/waqar-salamkheil-1108490",
    "/cricketers/kyle-phillip-1150684",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757",

    "/cricketers/aaron-finch-5334",
    "/cricketers/finn-allen-959759",
    "/cricketers/mackenzie-harvey-1076382",
    "/cricketers/smit-patel-532853",
    "/cricketers/matthew-wade-230193",
    "/cricketers/david-white-322812",
    "/cricketers/corey-anderson-277662",
    "/cricketers/chaitanya-bishnoi-628217",
    "/cricketers/sanjay-krishnamurthi-1277627",
    "/cricketers/shadab-khan-922943",
    "/cricketers/marcus-stoinis-325012",
    "/cricketers/tajinder-singh-1079860",
    "/cricketers/amila-aponso-429748",
    "/cricketers/brody-couch-1219972",
    "/cricketers/haris-rauf-1161606",
    "/cricketers/carmi-le-roux-440894",
    "/cricketers/lungi-ngidi-542023",
    "/cricketers/liam-plunkett-19264",
    "/cricketers/qais-ahmad-914171",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757",

    "/cricketers/aaron-jones-957645",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/shimron-hetmyer-670025",
    "/cricketers/heinrich-klaasen-436757",
    "/cricketers/nauman-anwar-788879",
    "/cricketers/harmeet-singh-422847",
    "/cricketers/imad-wasim-227758",
    "/cricketers/shehan-jayasuriya-422965",
    "/cricketers/nisarg-patel-233258",
    "/cricketers/angelo-perera-300629",
    "/cricketers/dwaine-pretorius-327830",
    "/cricketers/shubham-ranjane-554701",
    "/cricketers/dasun-shanaka-437316",
    "/cricketers/matthew-tromp-1339100",
    "/cricketers/sikandar-raza-299572",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/wayne-parnell-265564",
    "/cricketers/phani-simhadri-1386667",
    "/cricketers/andrew-tye-459508",
    "/cricketers/hayden-walsh-443263",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757",

    "/cricketers/faf-du-plessis-44828",
    "/cricketers/cody-chetty-322815",
    "/cricketers/devon-conway-379140",
    "/cricketers/lahiru-milantha-699483",
    "/cricketers/david-miller-321777",
    "/cricketers/saiteja-mukkamalla-1193310",
    "/cricketers/sami-aslam-547079",
    "/cricketers/dwayne-bravo-51439",
    "/cricketers/milind-kumar-451947",
    "/cricketers/mohammad-mohsin-935881",
    "/cricketers/daniel-sams-826901",
    "/cricketers/mitchell-santner-502714",
    "/cricketers/cameron-stevenson-953793",
    "/cricketers/zia-shahzad-1160390",
    "/cricketers/gerald-coetzee-596010",
    "/cricketers/imran-tahir-40618",
    "/cricketers/calvin-savage-450881",
    "/cricketers/rusty-theron-223642",
    "/cricketers/zia-ul-haq-290739",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757",

    "/cricketers/akhilesh-reddy-1252355",
    "/cricketers/andries-gous-485379",
    "/cricketers/sujith-gowda-1155264",
    "/cricketers/mukhtar-ahmed-556916",
    "/cricketers/josh-philippe-1124282",
    "/cricketers/glenn-phillips-823509",
    "/cricketers/saad-ali-448249",
    "/cricketers/justin-dill-594293",
    "/cricketers/moises-henriques-5961",
    "/cricketers/marco-jansen-696401",
    "/cricketers/obus-pienaar-328160",
    "/cricketers/wanindu-hasaranga-784379",
    "/cricketers/ben-dwarshuis-679567",
    "/cricketers/akeal-hosein-530812",
    "/cricketers/adam-milne-450860"
    "/cricketers/saurabh-netravalkar-398513",
    "/cricketers/anrich-nortje-481979",
    "/cricketers/dane-piedt-379926",
    "/cricketers/tanveer-sangha-1170471",
    "/cricketers/usman-rafiq-772469",
    "/cricketers/trent-boult-277912",
    "/cricketers/cameron-gannon-326635",
    "/cricketers/andrew-tye-459508",
    "/cricketers/nicholas-pooran-604302",
    "/cricketers/quinton-de-kock-379143",
    "/cricketers/heinrich-klaasen-436757"
]

left_out_player = {
    "A Raja": 681067,
    "AA Khan": 1241265,
    "AA Naseer": 1212829,
    "AA Nortje": 481979,
    "Adam Hose": 525992,
    "Alyssa Healy": 275486,
    "Andre Fletcher": 51862,
    "Andries Gous": 485379,
    "Arundhati": 960867,
    "Asha Sobhana": 550687,
    "Ashleigh Gardner": 858809,
    "B Hameed": 1209110,
    "B Mooney": 381258,
    "B de Leede": 1036191,
    "Benny Howell": 211748,
    "C Lynn": 326637,
    "C Savage": 450881,
    "C de Grandhomme": 55395,
    "Cameron Gannon": 326635,
    "Colin Ingram": 45705,
    "Corne Dry": 429927,
    "D Lawrence": 641423,
    "D Sharma": 597811,
    "Dane Piedt": 379926,
    "Dhara Gujjar": 1255408,
    "Disha Kasat": 961183,
    "Dominic Drakes": 906749,
    "E Adil": 547092,
    "Ellyse Perry": 275487,
    "Erin Burns": 275447,
    "F Farooqi": 974175,
    "GL Wareham": 946057,
    "Grace Harris": 381268,
    "Gus Atkinson": 1039481,
    "H Azam": 384518,
    "H Mathews": 474308,
    "H Rauf": 1161606,
    "H Zazai": 793457,
    "Heather Knight": 358259,
    "Humaira Kazi": 961005,
    "Issy Wong": 1146066,
    "J Clarke": 571911,
    "J Malhotra": 430105,
    "J Rodrigues": 883405,
    "JT Ball": 414990,
    "Jess Jonassen": 374936,
    "Jintimani Kalita": 1255387,
    "Jordan Thompson": 766809,
    "Justin Dill": 594293,
    "K Navgire": 1289983,
    "KA Sarvani": 960673,
    "KP Meiyappan": 1058987,
    "Kanika Ahuja": 1255560,
    "Komal Zanzad": 961195,
    "L Kumara": 784375,
    "L Wood": 573170,
    "Lahiru Milantha": 699483,
    "M Jawadullah": 1353033,
    "M Kumar": 451947,
    "M Patel": 1213438,
    "M Short": 605575,
    "M Ur Rahman": 974109,
    "M Waseem": 1241277,
    "MJ Siddique": 1203754,
    "Meg Lanning": 329336,
    "Muhammad Ali Khan": 927119,
    "N Kenjige": 1041679,
    "N Zadran": 524049,
    "NL Ahmad": 1182529,
    "NM Ul-Haq": 793447,
    "Natalie Sciver": 515905,
    "Niroshan Dickwella": 429754,
    "Nitish Kumar": 348129,
    "OJD Pope": 887207,
    "Obus Pienaar": 328160,
    "P Chopra": 1346678,
    "PW Hasaranga": 784379,
    "Poonam Khemnar": 1289979,
    "QK Ahmad": 914171,
    "R Ahmed": 1263691,
    "R Gayakwad": 709635,
    "R Gurbaz": 974087,
    "R Mustafa": 307808,
    "R Uthappa": 35582,
    "R Yadav": 960737,
    "RA Khan": 681067,
    "RJW Topley": 461632,
    "Raja Akifullah": 681067,
    "Ravi Bopara": 10582,
    "Ravi Rampaul": 52912,
    "Richa Ghosh": 1212830,
    "Rusty Theron": 223642,
    "S  Verma": 1182523,
    "S Badar": 922967,
    "S Jahangir": 647771,
    "S Netravalkar": 398513,
    "S Taylor": 348133,
    "S Yashasri": 1255485,
    "SB Raza": 299572,
    "SL Van Staden": 1070762,
    "SS Ladda": 317292,
    "SS Pandey": 442145,
    "SS Sehrawat": 1255442,
    "Samit Patel": 18632,
    "Shadley van Schalkwyk": 334621,
    "Shreyanka Patil": 1289948,
    "Sophie Devine": 231740,
    "T Mills": 459257,
    "Tahlia McGrath": 381311,
    "Tajinder Dhillon": 1079860,
    "Tanuja Kanwar": 960847,
    "Tara Norris": 872217,
    "Tom Kohler Cadmore": 470633,
    "Unmukt Chand": 446499,
    "W Smeed": 1099224,
    "Y Bhatia": 960715,
    "Yusuf Pathan": 32498,
    "Z Haq": 290739,
    "Z Khan": 384525
}


def get_duplicate():
    visited_dict = {}
    for url in WPL:
        id = url.split("/")[-1].split("-")[-1]
        if visited_dict.get(id):
            print(id)
        else:
            visited_dict[id] = 1

    for url in ILT20:
        id = url.split("/")[-1].split("-")[-1]
        if visited_dict.get(id):
            print(id)
        else:
            visited_dict[id] = 1

    for url in MLC:
        id = url.split("/")[-1].split("-")[-1]
        if visited_dict.get(id):
            print(id)
        else:
            visited_dict[id] = 1

    for key, value in left_out_player.items():
        if visited_dict.get(value):
            print(value)
        else:
            visited_dict[value] = 1


import json
import ssl

import requests


def get_player_skill():
    ids = []
    for player_url in WPL:
        player_cricinfo_id = player_url.split('-')[-1]
        ids.append(player_cricinfo_id)

    for player_url in ILT20:
        player_cricinfo_id = player_url.split('-')[-1]
        ids.append(player_cricinfo_id)

    for player_url in MLC:
        player_cricinfo_id = player_url.split('-')[-1]
        ids.append(player_cricinfo_id)

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    import pandas as pd

    # Define the column names
    columns = ['is_wicket_keeper', 'is_batsman', 'is_bowler', 'born', 'full_name', 'name',
               'short_name', 'cricinfo_id']

    # Create an empty DataFrame with the specified columns
    updated_df = pd.DataFrame(columns=columns)
    for id in ids:
        print(id)
        url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={id}"
        payload = {}
        headers = {}
        player_mapper_list = []
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            continue

        response = json.loads(response.text)
        player = response['player']
        playing_role = player['playingRoles']
        roles = []

        for role in playing_role:
            roles.extend(role.split(' '))

        # Create an empty DataFrame with the specified columns
        row = {}

        for role in roles:
            if role in ['wicketkeeper', 'wicketkeeper batter']:
                row['is_wicket_keeper'] = 1
            elif role in ['opening batter', 'batter', 'wicketkeeper batter', 'batting allrounder',
                          'top-order batter', 'middle-order batter']:
                row['is_batsman'] = 1
            elif role in ['bowler', 'bowling allrounder']:
                row['is_bowler'] = 1
            elif role == 'allrounder':
                row['is_batsman'] = 1
                row['is_bowler'] = 1
        if player['dateOfBirth']:
            row[
                'born'] = f"{player['dateOfBirth']['date']}-{player['dateOfBirth']['month']}-{player['dateOfBirth']['year']}"
        row['full_name'] = player['fullName']
        row['name'] = player['name']
        row['nvplay_name'] = player['name']
        row['short_name'] = player['mobileName']
        row['cricinfo_id'] = id

        updated_df = updated_df.append(row, ignore_index=True)
    updated_df = updated_df.fillna(-1)
    updated_df.to_csv("nvplay.csv")


def get_player_skill_left_out():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    import pandas as pd

    # Define the column names
    columns = ['is_wicket_keeper', 'is_batsman', 'is_bowler', 'born', 'full_name', 'name',
               'short_name', 'cricinfo_id']

    # Create an empty DataFrame with the specified columns
    updated_df = pd.DataFrame(columns=columns)
    for key, id in left_out_player.items():
        print(id)
        url = f"https://hs-consumer-api.espncricinfo.com/v1/pages/player/home?playerId={id}"
        payload = {}
        headers = {}
        player_mapper_list = []
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            continue

        response = json.loads(response.text)
        player = response['player']
        playing_role = player['playingRoles']
        roles = []

        for role in playing_role:
            roles.extend(role.split(' '))

        # Create an empty DataFrame with the specified columns
        row = {}

        for role in roles:
            if role in ['wicketkeeper', 'wicketkeeper batter']:
                row['is_wicket_keeper'] = 1
            elif role in ['opening batter', 'batter', 'wicketkeeper batter', 'batting allrounder',
                          'top-order batter', 'middle-order batter']:
                row['is_batsman'] = 1
            elif role in ['bowler', 'bowling allrounder']:
                row['is_bowler'] = 1
            elif role == 'allrounder':
                row['is_batsman'] = 1
                row['is_bowler'] = 1
        if player['dateOfBirth']:
            row[
                'born'] = f"{player['dateOfBirth']['date']}-{player['dateOfBirth']['month']}-{player['dateOfBirth']['year']}"
        row['full_name'] = player['fullName']
        row['name'] = player['name']
        row['nvplay_name'] = key
        row['short_name'] = player['mobileName']
        row['cricinfo_id'] = id

        updated_df = updated_df.append(row, ignore_index=True)
    updated_df = updated_df.fillna(-1)
    updated_df.to_csv("nvplay_left_out.csv")


def all_player_mapper_to_mapping_db_table():
    # cricinfo data
    nvplay_csv = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/nvplay.csv"
    )
    nvplay_left_out_csv = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/nvplay_left_out.csv"
    )
    nvplay_csv = nvplay_csv.drop(['Unnamed: 0'], axis=1).drop_duplicates()
    nvplay_left_out_csv = nvplay_left_out_csv.drop(['Unnamed: 0'], axis=1).drop_duplicates()


    common_among_both = pd.merge(
        nvplay_left_out_csv,
        nvplay_csv,
        on='cricinfo_id',
        how='inner',
    )
    remove_list = common_among_both['cricinfo_id'].tolist()
    # Create a boolean mask to identify rows with IDs to remove
    mask = ~nvplay_csv['cricinfo_id'].isin(remove_list)
    # Filter the DataFrame using the mask
    nvplay_csv = nvplay_csv[mask]
    nvplay_csv_final = pd.concat([nvplay_csv, nvplay_left_out_csv])
    nvplay_csv_final_cricinfo_id = nvplay_csv_final[['cricinfo_id']]
    players_mapping_df = getPandasFactoryDF(session, GET_PLAYER_MAPPER_SQL)[['cricinfo_id']]
    superset = players_mapping_df.reset_index(drop=True)
    subset = nvplay_csv_final_cricinfo_id.reset_index(drop=True)
    superset.replace("", pd.NA, inplace=True)
    superset = superset.dropna()
    superset['cricinfo_id'] = superset['cricinfo_id'].astype(int)
    subset = subset[['cricinfo_id']].drop_duplicates()
    x = pd.merge(
        subset,
        superset,
        on='cricinfo_id',
        how='left',
        indicator=True
    )
    already_mapping_exists = x[x['_merge'] == 'both']
    new_mapping_exists = x[x['_merge'] == 'left_only']
    already_mapping_exists = pd.merge(
        already_mapping_exists,
        nvplay_csv_final,
        on='cricinfo_id',
        how='left'
    )
    already_mapping_exists = already_mapping_exists[['cricinfo_id', 'nvplay_name']]
    already_mapping_exists.to_csv("already_mapping_exists.csv")
    new_mapping_exists = pd.merge(
        new_mapping_exists,
        nvplay_csv_final,
        on='cricinfo_id',
        how='left'
    )
    new_mapping_exists = new_mapping_exists[['cricinfo_id', 'nvplay_name']]
    new_mapping_exists.to_csv("new_mapping_exists.csv")


def generate_nvplay_player_id():
    def create_short_hash(player_name):
        import hashlib
        if type(player_name) != str:
            return ""
        # Create a hash object using SHA-256
        sha256_hash = hashlib.sha256(player_name.encode()).hexdigest()
        return f"nv_{sha256_hash}"

    already_mapping_exists = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/already_mapping_exists.csv"
    )
    already_mapping_exists = already_mapping_exists.drop(['Unnamed: 0'], axis=1).drop_duplicates()

    new_mapping_exists = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/new_mapping_exists.csv"
    )
    new_mapping_exists = new_mapping_exists.drop(['Unnamed: 0'], axis=1).drop_duplicates()

    for index, row in already_mapping_exists.iterrows():
        already_mapping_exists.at[index, 'nvplay_id'] = create_short_hash(row['nvplay_name'])

    for index, row in new_mapping_exists.iterrows():
        new_mapping_exists.at[index, 'nvplay_id'] = create_short_hash(row['nvplay_name'])

    already_mapping_exists.to_csv("already_mapping_exists_final.csv")
    new_mapping_exists.to_csv("new_mapping_exists_final.csv")


def final_mapping_001():
    already_mapping_exists = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/already_mapping_exists_final.csv"
    )
    already_mapping_exists = already_mapping_exists.drop(['Unnamed: 0'], axis=1).drop_duplicates()

    new_mapping_exists = readCSV(
        "/Users/achintya.chaudhary/Documents/projects/CricketDataService_Backup/DataIngestion/sources/nvplay/new_mapping_exists_final.csv"
    )
    new_mapping_exists = new_mapping_exists.drop(['Unnamed: 0'], axis=1).drop_duplicates()
    # To be called and used for PLAYER MAPPING
    concat_player_df = pd.concat([already_mapping_exists, new_mapping_exists]).drop_duplicates(['cricinfo_id'])
    concat_player_df.to_csv("concat_player_df.csv")


if __name__ == '__main__':
    get_duplicate()
    get_player_skill()
    get_player_skill_left_out()
    all_player_mapper_to_mapping_db_table()
    # generate_nvplay_player_id()
    final_mapping_001()
