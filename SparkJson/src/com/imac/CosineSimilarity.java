package com.imac;

import java.util.*;
import java.lang.Math;

public class CosineSimilarity {

	public static Double calculateCosineSimilarity(HashMap<String, Double> firstFeatures, HashMap<String, Double> secondFeatures) {
		Double similarity = 0.0;
		Double sum = 0.0;	// the numerator of the cosine similarity
		Double fnorm = 0.0;	// the first part of the denominator of the cosine similarity
		Double snorm = 0.0;	// the second part of the denominator of the cosine similarity
		Set<String> fkeys = firstFeatures.keySet();
		Iterator<String> fit = fkeys.iterator();
		
		while (fit.hasNext()) {
			String featurename = fit.next();
			boolean containKey = secondFeatures.containsKey(featurename);
			if (containKey) {
				sum = sum + firstFeatures.get(featurename) * secondFeatures.get(featurename);
//				System.out.println("sum  "+sum);
			}
		}
		fnorm = calculateNorm(firstFeatures);
		snorm = calculateNorm(secondFeatures);
		similarity = sum / (fnorm * snorm);
		return similarity;
	}
	
	public static Double calculateNorm(HashMap<String, Double> feature) {
		Double norm = 0.0;
		Set<String> keys = feature.keySet();
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String featurename = it.next();
			norm = norm + Math.pow(feature.get(featurename), 2);
		}
		return Math.sqrt(norm);
	}
	
}