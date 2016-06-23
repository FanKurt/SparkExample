package com.imac.elevator;

import java.util.Date;

public class Elevator_Module {
	public static String ERROR_STRING="自動運轉時馬達啟動失敗,自動運轉時馬達反轉,PG校正誤差超過100Plus,電梯行駛中，門迴路跳脫,安全迴路跳脫,手動開關動作,向下水平歸位,向上水平歸位,LEVELING FAILURE,執行尋樓,火災運轉結束,執行開門檢查，但車廂位在水平遮板,高速運轉超過設定時間,實際樓層超出程式極限樓層,叫車超過最大樓層,停電時到樓復歸失敗,UPS動作,停電或關機,來電開機,通信埠HC異常,通信埠Car異常,警鈴動作,地震動作,上極限動作,上減速動作,下減速動作,下極限動作,火災運轉開始,電梯超載,執行參數復歸,執行指撥 1復歸,執行指撥 1,4復歸,執行指撥 3復歸,執行指撥 3,4復歸,執行下光電上行補正,執行上光電下行補正,開門器開門過載,開門器關門過載,        ,關門時間過長,自動開門時，車廂未在水平遮板內,顯示最低樓層，但 DS未動作,顯示未在最低樓層，但 DS動作,顯示最高樓層，但 US未動作,顯示未在最高樓層，但 US動作,US和DS訊號同時觸發,前後單開模式，取消當樓叫車,門無法開啟執行次樓停靠,電梯門無法開到底超過時效,MC2沾黏保護動作,淹水感知器動作,電動機招回啟動,Parking開關動作,水平歸位失敗,開門停機動作,VIP動作,電梯關人GSM回報,電梯故障GSM回報,外叫面板按鈕卡鍵,MC1沾黏保護動作,BK沾黏保護,        ,        ,BOOT ERROR,待機時ROP動作,待機時ROP動作,高速時速度不在比例範圍,尋樓時編碼器反向,尋樓時水平下光電未離開，下極限動作,        ,尋樓時水平上光電未離開，上極限動作,下預備停車距離過大,上預備停車距離過大,井道安全[調速機][上安全][下安全]動作,PIT 機坑安全動作,CE 車廂安全動作,向上爬行時間過長,向下爬行時間過長,EA變頻器安全動作,操作盤手動開關動作,車廂手動開關動作,K2安全繼電器粘粘短路,BK沾黏保護LA動作,BK沾黏保護LB動作,BK沾黏保護RA動作,BK動作確認RB故障,電梯行駛中，內門迴路跳脫,電梯行駛中，外門迴路跳脫,開門時未在水平區,開門時電梯仍有速度";
	public static String getNowTime() {
		Date now = new Date();
		String[] token = now.toString().split(" ");
		String [] token2 = token[3].split(":");
		String update_time = (Integer.parseInt(token2[0]))+":"+token2[1]+":"+token2[2];
		
		String time = token[token.length - 1] + "-" + tranferMonth(token[1])
				+ "-" + token[2] + "T" + update_time + "Z";
		return time;
	}

	public static String tranferMonth(String month) {
		String[] arrStrings = { "Jan", "Feb", "Mar", "Apr", "Mar", "Jun",
				"Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		for (int i = 0; i < arrStrings.length; i++) {
			if (month.equals(arrStrings[i])) {
				return (i < 9) ? "0" + (i + 1) : "" + (i + 1);
			}
		}
		return "";
	}
}
