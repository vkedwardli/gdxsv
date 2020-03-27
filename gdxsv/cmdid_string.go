// Code generated by "stringer -type=CmdID"; DO NOT EDIT.

package main

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[CMD_LineCheck-24577]
	_ = x[CMD_Logout-24578]
	_ = x[CMD_ShutDown-24579]
	_ = x[CMD_VSUserLost-24580]
	_ = x[CMD_SendMail-26372]
	_ = x[CMD_Mail-26373]
	_ = x[CMD_ManagerMessage-26374]
	_ = x[CMD_LoginType-24848]
	_ = x[CMD_ConnectionId-24833]
	_ = x[CMD_AskConnectionId-24834]
	_ = x[CMD_WarningMessage-24835]
	_ = x[CMD_RegulationHeader-26656]
	_ = x[CMD_RegulationText-26657]
	_ = x[CMD_RegulationFooter-26658]
	_ = x[CMD_UserHandle-24849]
	_ = x[CMD_UserRegist-24850]
	_ = x[CMD_AddProgress-24856]
	_ = x[CMD_AskBattleResult-24864]
	_ = x[CMD_AskGameVersion-24855]
	_ = x[CMD_AskGameCode-24854]
	_ = x[CMD_AskCountryCode-24853]
	_ = x[CMD_AskPlatformCode-24852]
	_ = x[CMD_AskKDDICharges-24898]
	_ = x[CMD_PostGameParameter-24899]
	_ = x[CMD_WinLose-24901]
	_ = x[CMD_RankRanking-24900]
	_ = x[CMD_DeviceData-24904]
	_ = x[CMD_ServerMoney-24905]
	_ = x[CMD_AskNewsTag-26625]
	_ = x[CMD_NewsText-26626]
	_ = x[CMD_InvitationTag-26640]
	_ = x[CMD_TopRankingTag-26705]
	_ = x[CMD_AskPatchData-26721]
	_ = x[CMD_PatchHeader-26722]
	_ = x[CMD_PatchData_6863-26723]
	_ = x[CMD_CalcDownloadChecksum-26724]
	_ = x[CMD_PatchPing-26725]
	_ = x[CMD_StartLobby-24897]
	_ = x[CMD_PlazaMax-25091]
	_ = x[CMD_PlazaTitle-25092]
	_ = x[CMD_PlazaJoin-25093]
	_ = x[CMD_PlazaStatus-25094]
	_ = x[CMD_PlazaExplain-25098]
	_ = x[CMD_PlazaExit-25350]
	_ = x[CMD_PlazaEntry-25095]
	_ = x[CMD_LobbyJoin-25347]
	_ = x[CMD_LobbyEntry-25349]
	_ = x[CMD_LobbyMatchingJoin-25615]
	_ = x[CMD_LobbyExit-25608]
	_ = x[CMD_RoomTitle-25602]
	_ = x[CMD_RoomStatus-25604]
	_ = x[CMD_RoomMax-25601]
	_ = x[CMD_RoomCreate-25607]
	_ = x[CMD_PutRoomName-26121]
	_ = x[CMD_EndRoomCreate-26124]
	_ = x[CMD_PostChatMessage-26369]
	_ = x[CMD_ChatMessage-26370]
	_ = x[CMD_UserSite-26371]
	_ = x[CMD_LobbyRemove-25792]
	_ = x[CMD_LobbyMatchingEntry-25614]
	_ = x[CMD_WaitJoin-25862]
	_ = x[CMD_RoomExit-25857]
	_ = x[CMD_RoomEntry-25606]
	_ = x[CMD_MatchingEntry-25860]
	_ = x[CMD_ReadyBattle-26896]
	_ = x[CMD_TopRankingSuu-26706]
	_ = x[CMD_TopRanking-26707]
}

const _CmdID_name = "CMD_LineCheckCMD_LogoutCMD_ShutDownCMD_VSUserLostCMD_ConnectionIdCMD_AskConnectionIdCMD_WarningMessageCMD_LoginTypeCMD_UserHandleCMD_UserRegistCMD_AskPlatformCodeCMD_AskCountryCodeCMD_AskGameCodeCMD_AskGameVersionCMD_AddProgressCMD_AskBattleResultCMD_StartLobbyCMD_AskKDDIChargesCMD_PostGameParameterCMD_RankRankingCMD_WinLoseCMD_DeviceDataCMD_ServerMoneyCMD_PlazaMaxCMD_PlazaTitleCMD_PlazaJoinCMD_PlazaStatusCMD_PlazaEntryCMD_PlazaExplainCMD_LobbyJoinCMD_LobbyEntryCMD_PlazaExitCMD_RoomMaxCMD_RoomTitleCMD_RoomStatusCMD_RoomEntryCMD_RoomCreateCMD_LobbyExitCMD_LobbyMatchingEntryCMD_LobbyMatchingJoinCMD_LobbyRemoveCMD_RoomExitCMD_MatchingEntryCMD_WaitJoinCMD_PutRoomNameCMD_EndRoomCreateCMD_PostChatMessageCMD_ChatMessageCMD_UserSiteCMD_SendMailCMD_MailCMD_ManagerMessageCMD_AskNewsTagCMD_NewsTextCMD_InvitationTagCMD_RegulationHeaderCMD_RegulationTextCMD_RegulationFooterCMD_TopRankingTagCMD_TopRankingSuuCMD_TopRankingCMD_AskPatchDataCMD_PatchHeaderCMD_PatchData_6863CMD_CalcDownloadChecksumCMD_PatchPingCMD_ReadyBattle"

var _CmdID_map = map[CmdID]string{
	24577: _CmdID_name[0:13],
	24578: _CmdID_name[13:23],
	24579: _CmdID_name[23:35],
	24580: _CmdID_name[35:49],
	24833: _CmdID_name[49:65],
	24834: _CmdID_name[65:84],
	24835: _CmdID_name[84:102],
	24848: _CmdID_name[102:115],
	24849: _CmdID_name[115:129],
	24850: _CmdID_name[129:143],
	24852: _CmdID_name[143:162],
	24853: _CmdID_name[162:180],
	24854: _CmdID_name[180:195],
	24855: _CmdID_name[195:213],
	24856: _CmdID_name[213:228],
	24864: _CmdID_name[228:247],
	24897: _CmdID_name[247:261],
	24898: _CmdID_name[261:279],
	24899: _CmdID_name[279:300],
	24900: _CmdID_name[300:315],
	24901: _CmdID_name[315:326],
	24904: _CmdID_name[326:340],
	24905: _CmdID_name[340:355],
	25091: _CmdID_name[355:367],
	25092: _CmdID_name[367:381],
	25093: _CmdID_name[381:394],
	25094: _CmdID_name[394:409],
	25095: _CmdID_name[409:423],
	25098: _CmdID_name[423:439],
	25347: _CmdID_name[439:452],
	25349: _CmdID_name[452:466],
	25350: _CmdID_name[466:479],
	25601: _CmdID_name[479:490],
	25602: _CmdID_name[490:503],
	25604: _CmdID_name[503:517],
	25606: _CmdID_name[517:530],
	25607: _CmdID_name[530:544],
	25608: _CmdID_name[544:557],
	25614: _CmdID_name[557:579],
	25615: _CmdID_name[579:600],
	25792: _CmdID_name[600:615],
	25857: _CmdID_name[615:627],
	25860: _CmdID_name[627:644],
	25862: _CmdID_name[644:656],
	26121: _CmdID_name[656:671],
	26124: _CmdID_name[671:688],
	26369: _CmdID_name[688:707],
	26370: _CmdID_name[707:722],
	26371: _CmdID_name[722:734],
	26372: _CmdID_name[734:746],
	26373: _CmdID_name[746:754],
	26374: _CmdID_name[754:772],
	26625: _CmdID_name[772:786],
	26626: _CmdID_name[786:798],
	26640: _CmdID_name[798:815],
	26656: _CmdID_name[815:835],
	26657: _CmdID_name[835:853],
	26658: _CmdID_name[853:873],
	26705: _CmdID_name[873:890],
	26706: _CmdID_name[890:907],
	26707: _CmdID_name[907:921],
	26721: _CmdID_name[921:937],
	26722: _CmdID_name[937:952],
	26723: _CmdID_name[952:970],
	26724: _CmdID_name[970:994],
	26725: _CmdID_name[994:1007],
	26896: _CmdID_name[1007:1022],
}

func (i CmdID) String() string {
	if str, ok := _CmdID_map[i]; ok {
		return str
	}
	return "CmdID(" + strconv.FormatInt(int64(i), 10) + ")"
}