package com.imac.elevator;

import java.util.Date;

public class Elevator_Module {
	public static String ERROR_STRING="�۰ʹB��ɰ��F�Ұʥ���,�۰ʹB��ɰ��F����,PG�ե��~�t�W�L100Plus,�q���p���A���j������,�w���j������,��ʶ}���ʧ@,�V�U�����k��,�V�W�����k��,LEVELING FAILURE,����M��,���a�B�൲��,����}���ˬd�A�����[��b�����B�O,���t�B��W�L�]�w�ɶ�,��ڼӼh�W�X�{�������Ӽh,�s���W�L�̤j�Ӽh,���q�ɨ�Ӵ_�k����,UPS�ʧ@,���q������,�ӹq�}��,�q�H��HC���`,�q�H��Car���`,ĵ�a�ʧ@,�a�_�ʧ@,�W�����ʧ@,�W��t�ʧ@,�U��t�ʧ@,�U�����ʧ@,���a�B��}�l,�q��W��,����Ѽƴ_�k,������� 1�_�k,������� 1,4�_�k,������� 3�_�k,������� 3,4�_�k,����U���q�W��ɥ�,����W���q�U��ɥ�,�}�����}���L��,�}���������L��,        ,�����ɶ��L��,�۰ʶ}���ɡA���[���b�����B�O��,��̧ܳC�Ӽh�A�� DS���ʧ@,��ܥ��b�̧C�Ӽh�A�� DS�ʧ@,��̰ܳ��Ӽh�A�� US���ʧ@,��ܥ��b�̰��Ӽh�A�� US�ʧ@,US�MDS�T���P��Ĳ�o,�e���}�Ҧ��A������ӥs��,���L�k�}�Ұ��榸�Ӱ��a,�q����L�k�}�쩳�W�L�ɮ�,MC2�g�H�O�@�ʧ@,�T���P�����ʧ@,�q�ʾ��ۦ^�Ұ�,Parking�}���ʧ@,�����k�쥢��,�}�������ʧ@,VIP�ʧ@,�q�����HGSM�^��,�q��G��GSM�^��,�~�s���O���s�d��,MC1�g�H�O�@�ʧ@,BK�g�H�O�@,        ,        ,BOOT ERROR,�ݾ���ROP�ʧ@,�ݾ���ROP�ʧ@,���t�ɳt�פ��b��ҽd��,�M�Ӯɽs�X���ϦV,�M�Ӯɤ����U���q�����}�A�U�����ʧ@,        ,�M�Ӯɤ����W���q�����}�A�W�����ʧ@,�U�w�ư����Z���L�j,�W�w�ư����Z���L�j,���D�w��[�ճt��][�W�w��][�U�w��]�ʧ@,PIT ���|�w���ʧ@,CE ���[�w���ʧ@,�V�W����ɶ��L��,�V�U����ɶ��L��,EA���W���w���ʧ@,�ާ@�L��ʶ}���ʧ@,���[��ʶ}���ʧ@,K2�w���~�q�����ߵu��,BK�g�H�O�@LA�ʧ@,BK�g�H�O�@LB�ʧ@,BK�g�H�O�@RA�ʧ@,BK�ʧ@�T�{RB�G��,�q���p���A�����j������,�q���p���A�~���j������,�}���ɥ��b������,�}���ɹq�褴���t��";
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
