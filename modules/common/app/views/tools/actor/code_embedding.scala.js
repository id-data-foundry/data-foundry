@(code : String)

var program = (function() {
	var actor, tgReply;
	var internalData = {};
	var internalParticipants = [];
	var internalDevices = [];
	var internalWearables = [];
	var internalClusters = [];
	var internalDatasets = [];
	let datasetFcts = function(id) {
		return {
			get: function(did, limit, start, end) {
				return JSON.parse(actor.getDatasetProxy(id).getEventData(did, limit, start, end));
			},
			log: function(did, act, d) {
				actor.getDatasetProxy(id).logEventData(did, act, JSON.stringify(d));
			},
			filter: function(did, limit, start, end) {
				return {
					stats: function(keys) {
						return JSON.parse(actor.getDatasetProxy(id).filter(did, limit, start, end).stats(keys));
					}
				}
			},
			stats: function(keys) {
				return JSON.parse(actor.getDatasetProxy(id).stats(keys));
			}
		}
	};
	let DF = {
		eventData: {
			get: function(did, limit, start, end) {
				return JSON.parse(actor.getDatasetProxy('').getEventData(did, limit, start, end));
			},
			log: function(did, act, d) {
				actor.getDatasetProxy('').logEventData(did, act, JSON.stringify(d));
			},
			from: datasetFcts
		},
		dataset : datasetFcts,
		entity: {
			getAll: function() {
				return JSON.parse(actor.getEntityResources());
			},
			getAllMatching: function(id) {
				return JSON.parse(actor.getEntityResourcesMatching(id));
			},
			get: function(id) {
				return JSON.parse(actor.getEntityResource(id));
			},
			update: function(id, d) {
				actor.updateEntityResource(id, JSON.stringify(d));
			},
			add: function(id, d) {
				actor.addEntityResource(id, JSON.stringify(d));
			},
			delete: function(id) {
				actor.deleteEntityResource(id);
			}
		},
		getParticipant: function(id) {
			return internalParticipants.filter(function(p) { return p.id == id || p.participant_id == id;})[0];
		},
		getDevice: function(id) {
			return internalDevices.filter(function(d) { return d.id == id || d.device_id == id;})[0];
		},
		getWearable: function(id) {
			return internalWearables.filter(function(d) { return d.id == id || d.wearable_id == id;})[0];
		},
		telegramResearchers: function(s) {
			actor.sendTelegramMessageToResearchers(s);
		},
		telegramReply: function(s) {
			if(tgReply) {
				actor.replyTelegramMessage(tgReply, s);
			}
		},
		telegramParticipant: function(p, s) {
			actor.sendTelegramMessageToParticipant(p, s);
		},
		oocsi: function(c, d) {
			actor.sendOOCSIMessage(c, JSON.stringify(d));
		},
		api: function(api, d) {
			return JSON.parse(actor.apiDispatch(api, JSON.stringify(d)));
		},
		print : function(s) {
			if(typeof s === 'object' && s !== null) {
				actor.print(JSON.stringify(s))
			} else {
				actor.print(s)
			}
		}
	};

	function configure(a) {
		internalData = JSON.parse(a.data);
		tgReply = internalData.chatId;
		internalParticipants = JSON.parse(a.participants);
		internalDevices = JSON.parse(a.devices);
		internalWearables = JSON.parse(a.wearables);
		internalClusters = JSON.parse(a.clusters);
		internalDatasets = JSON.parse(a.datasets);
		actor = a;
	}

	return function(actor) {
		configure(actor);
		var data = internalData;
		let participants = internalParticipants;
		let devices = internalDevices;
		let wearables = internalWearables;
		let clusters = internalClusters;
		let datasets = internalDatasets;
		
		// client side code
		@play.twirl.api.JavaScriptFormat.raw(code)
	}
})();