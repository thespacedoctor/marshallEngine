import os
import nose2
import shutil
import unittest
import yaml
from marshallEngine.utKit import utKit

from fundamentals import tools

su = tools(
    arguments={"settingsFile": None},
    docString=__doc__,
    logLevel="DEBUG",
    options_first=False,
    projectName="marshallEngine",
    defaultSettingsFile=False
)
arguments, settings, log, dbConn = su.setup()

# # load settings
# stream = file(
#     "/Users/Dave/.config/marshallEngine/marshallEngine.yaml", 'r')
# settings = yaml.load(stream)
# stream.close()

# SETUP AND TEARDOWN FIXTURE FUNCTIONS FOR THE ENTIRE MODULE
moduleDirectory = os.path.dirname(__file__)
utKit = utKit(moduleDirectory)
log2, dbConn2, pathToInputDir, pathToOutputDir = utKit.setupModule()
utKit.tearDownModule()

# load settings
from os.path import expanduser
home = expanduser("~")
stream = file(
    home + "/.config/marshallEngine/marshallEngine.yaml", 'r')
settings = yaml.load(stream)
stream.close()

import shutil
try:
    shutil.rmtree(pathToOutputDir)
except:
    pass
# COPY INPUT TO OUTPUT DIR
shutil.copytree(pathToInputDir, pathToOutputDir)

# Recursively create missing directories
if not os.path.exists(pathToOutputDir):
    os.makedirs(pathToOutputDir)

# xt-setup-unit-testing-files-and-folders


class test_marshall_lightcurves(unittest.TestCase):

    def test_marshall_lightcurves_function(self):

        from marshallEngine.lightcurves import marshall_lightcurves, _plot_one
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=28121353
        )
        filepath, currentMag, gradient = _plot_one(28121353, log, settings)

    def test_marshall_lightcurves_function2(self):

        from marshallEngine.lightcurves import marshall_lightcurves
        lc = marshall_lightcurves(
            log=log,
            dbConn=dbConn,
            settings=settings,
            transientBucketIds=[17, 21, 26, 35, 43, 48, 57, 73, 81, 85, 26787201, 26787202, 26787203, 28173350, 28173371, 28173379, 28173390, 28173405, 28173408, 28173409, 28173428, 28173431, 28174061, 28174288, 28174289, 28174292, 28174296, 28174298, 28177073, 28177093, 28177094, 28177101, 28177102, 28177103, 28177104, 28186206, 28186332, 28186333, 28186400, 28186405, 28186549, 28186556, 28209373, 28209376, 28209471, 28209598, 28209680, 28209703, 28213418, 28215192, 28216068, 28217670, 28218141, 28219894, 28221483, 28222019, 28223558, 28223685, 28223873, 28224076, 28224178, 28224338, 28224451, 28224486, 28224760, 28225233, 28225276, 28225319, 28225354, 28225850, 28227606, 28229285, 28229287, 28229290, 28229669, 28230537, 28231134, 28231167, 28231194, 28231267, 28231282, 28231297, 28231344, 28231367, 28231382, 28245269, 28245274, 28245306, 28248990, 28249012, 28249022, 28249026, 28249625, 28258042, 28258051, 28281107, 28311604, 28311605, 28311625, 28311645, 28311792, 28328861, 28344469, 28346029, 28375300, 28375372, 28375373, 28375374, 28375376, 28375377, 28413765, 28413776, 28413798, 28413874, 28413897, 28413920, 28414093, 28414094, 28432567, 28474770, 28474773, 28487399, 28487463, 28487533, 28499612, 28506917, 28584363, 28584571, 28591929, 28609924, 28631545, 28698746, 28721269, 28721443, 28721538, 28722036, 28722246, 28723417, 28731817, 28752248, 28779194, 28782374, 28782384, 28784481, 28784646, 28784718, 28784731, 28784748, 28789282, 28790436, 28790443, 28790453, 28790454, 28800489, 28800493, 28800626, 28805322, 28805324, 28805329, 28805957, 28805961, 28810674, 28810675, 28831086, 28833697, 28834738, 28834756, 28836005, 28836006, 28836007, 28851862, 28851864, 28851865, 28851871, 28851873, 28851875, 28851876, 28851879, 28851880, 28872936, 28982063, 28986065, 29010258, 29056403, 29056541, 29090092, 29090100, 29095308, 29095367, 29095466, 29095469, 29103499, 29124487, 29156143, 29327170, 29347567, 29364278, 29364762, 29368813, 29369127, 29371728, 29371859, 29372860, 29389311, 29390277, 29390596, 29390650, 29460442, 29460479, 29460542, 29531540, 29556884, 29561270, 29561719, 29615016, 29615413, 29627011, 29627015, 29627022, 29627024, 29627026, 29627028, 29631369, 29631376, 29631381, 29631384, 29631388, 29631392, 29631393, 29631394, 29631396, 29631577, 29659022, 29659025, 29659028, 29659030, 29659032, 29659035, 29659036, 29659348, 29662189, 29672480, 29677525, 29680200, 29680480, 29680753, 29680809, 29691730, 29692576, 29692864, 29692865, 29692870, 29692871, 29708403, 29708404, 29708408, 29708409, 29708410, 29708411, 29708412, 29716779, 29717707, 29719731, 29719745, 29727670, 29727689, 29727708, 29728979, 29728999, 29729000, 29729001, 29729002, 29729004, 29729005, 29729018, 29729020, 29729021, 29729022, 29729023, 29729026, 29729031, 29729032, 29729037, 29729044, 29729045, 29729046, 29729047, 29729048, 29729049, 29729050, 29729564, 29729690, 29729693, 29733947, 29733951, 29733955, 29733956, 29733957, 29733961, 29733962, 29733963, 29741083, 29741084, 29741085, 29742504, 29742505, 29742506, 29742510, 29742511, 29755395, 29755397, 29756159, 29763030, 29763761, 29763762, 29841595, 29841647, 29841740, 29841848, 29841905, 29842025, 29842223, 29842280, 29842489, 29842540, 29842643, 29842730, 29842754, 29845280, 29867750, 29878011, 29894595, 29906577, 29906623, 29906798, 29906956, 29907062, 29907206, 29912187, 29912220, 29912301, 29912321, 29944350, 29944351, 29944352, 29944353, 29944354, 29956342, 29959481, 29962728, 29970635, 29985020, 30005309, 30014094, 30020766, 30031442, 30031625, 30035343, 30035506, 30036988, 30037332, 30049851, 30050015, 30050358, 30063593, 30071389, 30071484, 30071560, 30087023, 30087158, 30087597, 30095171, 30099576, 30101956, 30106433, 30108555, 30110270, 30110354, 30110537, 30120942, 30121063, 30121176, 30123872, 30135091, 30135202, 30135778, 30135911, 30136552, 30143009, 30143249, 30143437, 30143544, 30143754, 30144011, 30148157, 30148276, 30148454, 30148607, 30148742, 30148948, 30149059, 30150289, 30150846, 30151284, 30151551, 30152572, 30152779, 30153106, 30154635, 30155797, 30156484, 30156892, 30157138, 30157627, 30157880, 30157882, 30157883, 30157884, 30157885, 30157886, 30157887, 30157888, 30157889, 30157890, 30157891, 30157892, 30157893, 30157894, 30157895, 30157896, 30157897, 30157898, 30157899, 30157900, 30157901, 30157902, 30157904, 30157906, 30157907, 30157908, 30157909, 30158053, 30158055, 30158841, 30159265, 30159752, 30159995, 30161156, 30161439, 30162232, 30162375, 30163365, 30163424, 30163513, 30163744, 30163943, 30165048, 30165220, 30165333, 30165496, 30165916, 30166048, 30166053, 30166070, 30174798, 30179125, 30179561, 30185222, 30185237, 30185279, 30185341, 30185362, 30188029, 30188042, 30188061, 30200733, 30200738, 30200740, 30200742, 30205954, 30205960, 30205961, 30205962, 30205963, 30205964, 30205965, 30205966, 30205967, 30205968, 30205969, 30205970, 30205971, 30205972, 30205973, 30205974, 30205975, 30205976, 30205977, 30205980, 30205982, 30205983, 30205984, 30205985, 30205986, 30205987, 30205988, 30205989, 30205991, 30205992, 30205993, 30205994, 30205995, 30205996, 30205997, 30205998, 30205999, 30206000, 30206001, 30206002, 30206003, 30206004, 30206005, 30206006, 30206007, 30206008, 30206009, 30206010, 30206011, 30206012, 30206013, 30206014, 30206015, 30206016, 30206017, 30206018, 30206019, 30206020, 30206021, 30206022, 30206023, 30206025, 30206026, 30206027, 30206029, 30206030, 30206031, 30206032, 30206033, 30206034, 30206035, 30206036, 30206038, 30206039, 30206043, 30206045, 30206046, 30206051, 30206059, 30206062, 30206064, 30206067, 30206068, 30206071, 30206073, 30206081, 30207367, 30207385, 30207427, 30207442, 30207457, 30207487, 30208687, 30208691, 30208726, 30208747, 30208756, 30208793, 30208797, 30208810, 30208811, 30208815, 30208823, 30222510, 30222516, 30222531, 30222536, 30222545, 30222547, 30223681, 30223706, 30223710, 30223711, 30223712, 30223713, 30223714, 30223715, 30223716, 30223717, 30223718, 30223719, 30223720, 30223721, 30223722, 30223724, 30233047, 30233048, 30233050, 30233051, 30233052, 30233053, 30233054, 30233055, 30233056, 30233057, 30233058, 30233059, 30233060, 30233061, 30233062, 30233063, 30233064, 30233065, 30233066, 30233068, 30233071, 30233072, 30233073, 30233074, 30233075, 30233076, 30233079, 30233118, 30233120, 30233127, 30233136, 30233138, 30233242, 30233243, 30233257, 30235071, 30250645, 30250646, 30250647, 30250648, 30250649, 30250650, 30250651, 30250652, 30250653, 30250663, 30250665, 30250666, 30252900, 30252902, 30321459, 30321475, 30328767, 30328769, 30328771, 30339962, 30339984, 30345567, 30345569, 30384342, 30384343, 30384344, 30384349, 30400541, 30400543, 30406663, 30406665, 30406667, 30406669, 30406671, 30406674, 30448055, 30470918, 30480924, 30481221, 30481324, 30481407, 30481704, 30481762, 30487778, 30493130, 30493131, 30493132, 30493133, 30493138, 30493139, 30493140, 30493141, 30493142, 30493143, 30493144, 30493145, 30493146, 30532869, 30532894, 30533538, 30544542, 30554249, 30555139, 30558863, 30558971, 30559009, 30560809, 30560846, 30565202, 30565456, 30565620, 30565788, 30565975, 30570606, 30571108, 30571289, 30577876, 30578287, 30579816, 30580317, 30580406, 30582019, 30582163, 30585416, 30585722, 30585789, 30585885, 30585906, 30586043, 30586467, 30586836, 30587183, 30587208, 30587242, 30587467, 30587617, 30587829, 30587995, 30588032, 30588101, 30588469, 30588556, 30588857, 30588920, 30614780, 30614838, 30628746, 30628769, 30628782, 30628793, 30632581, 30632583, 30632584, 30632587, 30632588, 30632589, 30632590, 30632811, 30644428, 30644430, 30644432, 30650227, 30650617, 30650619, 30650620, 30652371, 30652375, 30652384, 30652387, 30652390, 30652395, 30653252, 30665290, 30666623, 30666745, 30666791, 30666831, 30666955, 30666977, 30671324, 30672009, 30672527, 30677934, 30677935, 30677936, 30677937, 30677942, 30678185, 30678201, 30679028, 30679038, 30679092, 30679101, 30679112, 30679132, 30679140, 30679978, 30679980, 30688880, 30688881, 30688882, 30688883, 30688884, 30688885, 30688886, 30688887, 30689072, 30689113, 30689122, 30689124, 30709864, 30709868, 30709869, 30709870, 30709872, 30709874, 30709877, 30709878, 30709879, 30709880, 30709881, 30709882, 30709883, 30709885, 30709888, 30709889, 30709890, 30709891, 30709918, 30709919, 30709922, 30709924, 30710778, 30710850, 30710873, 30712619, 30712620, 30712621, 30712622, 30712623, 30712917, 30712928, 30713096, 30713097, 30713100, 30713113, 30713148, 30715023, 30715027, 30715029, 30715030, 30715031, 30715032, 30715033, 30715034, 30715035, 30715036, 30715037, 30715038, 30715043, 30715064, 30715073, 30715157, 30715178, 30715187, 30715200, 30715215, 30715257, 30715325, 30715385, 30715414, 30715432, 30715465, 30715483, 30716182, 30716250, 30716289, 30716303, 30716313, 30717172, 30717181, 30717229, 30717262, 30717295, 30717319, 30717332, 30717365, 30722574, 30722607, 30722628, 30722679, 30722718, 30722728, 30722736, 30722744, 30722765, 30722819, 30722835, 30722874, 30722931, 30722946, 30722961, 30722992, 30723013, 30723034, 30723037, 30723061, 30723092, 30723119, 30723136, 30723209, 30724707, 30724728, 30724765, 30724817,
                                30724828, 30724854, 30724906, 30724942, 30730670, 30730671, 30730672, 30730673, 30730674, 30730675, 30730676, 30730677, 30730678, 30730679, 30730680, 30730681, 30730682, 30730683, 30730684, 30730685, 30730686, 30730687, 30730688, 30730689, 30730690, 30730691, 30730692, 30730693, 30730694, 30730695, 30730696, 30730697, 30730698, 30730699, 30730700, 30730701, 30730702, 30730703, 30730704, 30730705, 30730706, 30730707, 30730708, 30730709, 30730710, 30730711, 30730712, 30730713, 30730714, 30730715, 30730716, 30730717, 30730718, 30730719, 30730720, 30730721, 30730722, 30730723, 30730724, 30730725, 30730726, 30730727, 30730728, 30730729, 30730730, 30730731, 30730732, 30730733, 30730734, 30730735, 30730736, 30730737, 30730738, 30730739, 30730740, 30730741, 30730742, 30730743, 30730744, 30730745, 30730746, 30730747, 30730748, 30730749, 30730750, 30730751, 30730752, 30730753, 30730754, 30730755, 30730756, 30730757, 30730758, 30730759, 30730760, 30730761, 30730762, 30730763, 30730764, 30730765, 30730766, 30730767, 30730768, 30730769, 30730770, 30730771, 30730772, 30730773, 30730774, 30730775, 30730776, 30730777, 30730778, 30730779, 30730780, 30730781, 30730782, 30730783, 30730784, 30730785, 30730786, 30730787, 30730788, 30730789, 30730790, 30730791, 30730792, 30730793, 30730794, 30730795, 30730796, 30730797, 30730798, 30730799, 30730800, 30730801, 30730802, 30730803, 30730804, 30730805, 30730806, 30730807, 30730808, 30730809, 30730810, 30730811, 30730812, 30730813, 30730814, 30730815, 30730816, 30730817, 30730818, 30730819, 30730820, 30730821, 30730822, 30730823, 30730824, 30730825, 30730826, 30730827, 30730828, 30730829, 30730830, 30730831, 30730832, 30730833, 30730834, 30730835, 30730836, 30730837, 30730838, 30730839, 30730840, 30730841, 30730842, 30730843, 30730844, 30730845, 30730846, 30730847, 30730848, 30730849, 30730850, 30730851, 30730852, 30730853, 30730854, 30730855, 30730856, 30730857, 30730858, 30730859, 30730860, 30730861, 30730862, 30730863, 30730864, 30730865, 30730866, 30730867, 30730868, 30730869, 30730870, 30730871, 30730872, 30730873, 30730874, 30730875, 30730876, 30730877, 30730878, 30730879, 30730880, 30730881, 30730882, 30730883, 30730884, 30730885, 30730886, 30730887, 30730888, 30730889, 30730890, 30730891, 30730892, 30730893, 30730894, 30730895, 30730896, 30730897, 30730898, 30730899, 30730900, 30730901, 30730902, 30730903, 30730904, 30730905, 30730906, 30730907, 30730908, 30730909, 30730910, 30730911, 30730912, 30730913, 30730914, 30730915, 30730916, 30730917, 30730918, 30730919, 30730920, 30730921, 30730922, 30730923, 30730924, 30730925, 30730926, 30730927, 30730928, 30730929, 30730930, 30730931, 30730932, 30730933, 30730934, 30730935, 30730936, 30730937, 30730938, 30730939, 30730940, 30730941, 30730942, 30730943, 30730944, 30730945, 30730946, 30730947, 30730948, 30730949, 30730950, 30730951, 30730952, 30730953, 30730954, 30730955, 30730956, 30730957, 30730958, 30730959, 30730960, 30730961, 30730962, 30730963, 30730964, 30730965, 30730966, 30730967, 30730968, 30730969, 30730970, 30730971, 30730972, 30730973, 30730974, 30730975, 30730976, 30730977, 30730978, 30730979, 30730980, 30730981, 30730982, 30730983, 30730984, 30730985, 30730986, 30730987, 30730988, 30730989, 30730990, 30730991, 30730992, 30730993, 30730994, 30730995, 30730996, 30730997, 30730998, 30730999, 30731000, 30731001, 30731002, 30731003, 30731004, 30731005, 30731006, 30731007, 30731008, 30731009, 30731010, 30731011, 30731012, 30731013, 30731014, 30731015, 30731016, 30731017, 30731018, 30731019, 30731020, 30731021, 30731022, 30731023, 30731024, 30731025, 30731026, 30731027, 30731028, 30731029, 30731030, 30731031, 30731032, 30731033, 30731034, 30731035, 30731036, 30731037, 30731038, 30731039, 30731040, 30731041, 30731042, 30731043, 30731044, 30731045, 30731046, 30731047, 30731048, 30731049, 30731050, 30731051, 30731052, 30731053, 30731054, 30731055, 30731056, 30731057, 30731058, 30731059, 30731060, 30731061, 30731062, 30731063, 30731064, 30731065, 30731066, 30731067, 30731068, 30731069, 30731070, 30731071, 30731072, 30731073, 30731074, 30731075, 30731076, 30731077, 30731078, 30731079, 30731080, 30731081, 30731082, 30731083, 30731084, 30731085, 30731086, 30731087, 30731088, 30731089, 30731090, 30731091, 30731092, 30731093, 30731094, 30731095, 30731096, 30731097, 30731098, 30731099, 30731100, 30731101, 30731102, 30731103, 30731104, 30731105, 30731106, 30731107, 30731108, 30731109, 30731110, 30731111, 30731112, 30731113, 30731114, 30731115, 30731116, 30731117, 30731118, 30731119, 30731120, 30731121, 30731122, 30731123, 30731124, 30731125, 30731126, 30731127, 30731128, 30731129, 30731130, 30731131, 30731132, 30731133, 30731134, 30731135, 30731136, 30731137, 30731138, 30731139, 30731140, 30731141, 30731142, 30731143, 30731144, 30731145, 30731146, 30731147, 30731148, 30731149, 30731150, 30731151, 30731152, 30731153, 30731154, 30731155, 30731156, 30731157, 30731158, 30731159, 30731160, 30731161, 30731162, 30731163, 30731164, 30731165, 30731166, 30731167, 30731168, 30731169, 30731170, 30731171, 30731172, 30731173, 30731174, 30731175, 30731176, 30731177, 30731178, 30731179, 30731180, 30731181, 30731182, 30731183, 30731184, 30731185, 30731186, 30731187, 30731188, 30731189, 30731190, 30731191, 30731192, 30731193, 30731194, 30731195, 30731196, 30731197, 30731198, 30731199, 30731200, 30731201, 30731202, 30731203, 30731204, 30731205, 30731206, 30731207, 30731208, 30731209, 30731210, 30731211, 30731212, 30731213, 30731214, 30731215, 30731216, 30731217, 30731218, 30731219, 30731220, 30731221, 30731222, 30731223, 30731224, 30731225, 30731226, 30731227, 30731228, 30731229, 30731230, 30731231, 30731232, 30731233, 30731234, 30731235, 30731236, 30731237, 30731238, 30731239, 30731240, 30731241, 30731242, 30731243, 30731244, 30731245, 30731246, 30731247, 30731248, 30731249, 30731250, 30731251, 30731252, 30731253, 30731254, 30731255, 30731256, 30731257, 30731258, 30731259, 30731260, 30731261, 30731262, 30731263, 30731264, 30731265, 30731266, 30731267, 30731268, 30731269, 30731270, 30731271, 30731272, 30731273, 30731274, 30731275, 30731276, 30731277, 30731278, 30731279, 30731280, 30731281, 30731282, 30731283, 30731284, 30731285, 30731286, 30731287, 30731288, 30731289, 30731290, 30731291, 30731292, 30731293, 30731294, 30731295, 30731296, 30731297, 30731298, 30731299, 30731300, 30731301, 30731302, 30731303, 30731304, 30731305, 30731306, 30731307, 30731308, 30731309, 30731310, 30731311, 30731312, 30731313, 30731314, 30731315, 30731316, 30731317, 30731318, 30731319, 30731320, 30731321, 30731322, 30731323, 30731324, 30731325, 30731326, 30731327, 30731328, 30731329, 30731330, 30731331, 30731332, 30731333, 30731334, 30731335, 30731336, 30731337, 30731338, 30731339, 30731340, 30731341, 30731342, 30731343, 30731344, 30731345, 30731346, 30731347, 30731348, 30731349, 30731350, 30731351, 30731352, 30731353, 30731354, 30731355, 30731356, 30731357, 30731358, 30731359, 30731360, 30731361, 30731362, 30731363, 30731364, 30731365, 30731366, 30731367, 30731368, 30731369, 30731370, 30731371, 30731372, 30731373, 30731374, 30731375, 30731376, 30731377, 30731378, 30731379, 30731380, 30731381, 30731382, 30731383, 30731384, 30731385, 30731386, 30731387, 30731388, 30731389, 30731390, 30731391, 30731392, 30731393, 30731394, 30731395, 30731396, 30731397, 30731398, 30731399, 30731400, 30731401, 30731402, 30731403, 30731404, 30731405, 30731406, 30731407, 30731408, 30731409, 30731410, 30731411, 30731412, 30731413, 30731414, 30731415, 30731416, 30731417, 30731418, 30731419, 30731420, 30731421, 30731422, 30731423, 30731424, 30731425, 30731426, 30731427, 30731428, 30731429, 30731430, 30731431, 30731432, 30731433, 30731434, 30731435, 30731436, 30731437, 30731438, 30731439, 30731440, 30731441, 30731442, 30731443, 30731444, 30731445, 30731446, 30731447, 30731448, 30731449, 30731450, 30731451, 30731452, 30731453, 30731454, 30731455, 30731456, 30731457, 30731458, 30731459, 30731460, 30731461, 30731462, 30731463, 30731464, 30731465, 30731466, 30731467, 30731468, 30731469, 30731470, 30731471, 30731472, 30731473, 30731474, 30731475, 30731476, 30731477, 30731478, 30731479, 30731480, 30731481, 30731482, 30731483, 30731484, 30731485, 30731486, 30731487, 30731488, 30731489, 30731490, 30731491, 30731492, 30731493, 30731494, 30731495, 30731496, 30731497, 30731498, 30731499, 30731500, 30731501, 30731502, 30731503, 30731504, 30731505, 30731506, 30731507, 30731508, 30731509, 30731510, 30731511, 30731512, 30731513, 30731514, 30731515, 30731516, 30731517, 30731518, 30731519, 30731520, 30731521, 30731522, 30731523, 30731524, 30731525, 30731526, 30731527, 30731528, 30731529, 30731530, 30731531, 30731532, 30731533, 30731534, 30731535, 30731536, 30731537, 30731538, 30731539, 30731540, 30731541, 30731542, 30731543, 30731544, 30731545, 30731546, 30731547, 30731548, 30731549, 30731550, 30731551, 30731552, 30731553, 30731554, 30731555, 30731556, 30731557, 30731558, 30731559, 30731560, 30731561, 30731562, 30731563, 30731564, 30731565, 30731566, 30731567, 30731568, 30731569, 30731570, 30731571, 30731572, 30731573, 30731663, 30731664, 30731665]
        )
        this = lc.plot()
        print this

    def test_marshall_lightcurves_function_exception(self):

        from marshallEngine.lightcurves import marshall_lightcurves
        try:
            this = marshall_lightcurves(
                log=log,
                settings=settings,
                fakeKey="break the code"
            )
            this.get()
            assert False
        except Exception, e:
            assert True
            print str(e)

        # x-print-testpage-for-pessto-marshall-web-object

    # x-class-to-test-named-worker-function
