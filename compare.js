const fs = require("fs");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// 입력 CSV 파일 경로
const inputFilePath = "input.csv";
// 출력 CSV 파일 경로
const outputFilePath = "output.csv";

// CSV 파일을 읽어서 데이터를 배열로 변환하는 함수
const readCSV = (filePath) => {
	return new Promise((resolve, reject) => {
		const results = [];
		fs.createReadStream(filePath)
			.pipe(csv())
			.on("data", (data) => results.push(data))
			.on("end", () => {
				resolve(results);
			})
			.on("error", (error) => {
				reject(error);
			});
	});
};

// CSV 파일에 데이터를 쓰는 함수
const writeCSV = (filePath, data) => {
	const csvWriter = createCsvWriter({
		path: filePath,
		header: [
			{ id: "time1", title: "time1" },
			{ id: "time2", title: "time2" },
		],
	});

	return csvWriter.writeRecords(data);
};

// 두 시간을 비교하여 10분 이내로 일치하는 값을 찾는 함수
const findMatchingTimes = (data) => {
	const matchedData = [];

	for (let i = 0; i < data.length; i++) {
		const time1 = new Date(data[i].time1);

		for (let j = 0; j < data.length; j++) {
			if (i !== j) {
				const time2 = new Date(data[j].time2);
				const diff = Math.abs(time1 - time2) / (1000 * 60); // 시간 차이를 분으로 변환

				if (diff <= 10) {
					matchedData.push({ time1: data[i].time1, time2: data[j].time2 });
				}
			}
		}
	}

	return matchedData;
};

// 메인 함수
const main = async () => {
	try {
		// 입력 CSV 파일 읽기
		const inputData = await readCSV(inputFilePath);

		// 10분 이내로 일치하는 값을 찾기
		const matchedData = findMatchingTimes(inputData);

		// 출력 CSV 파일에 데이터 쓰기
		await writeCSV(outputFilePath, matchedData);

		console.log("Matching times saved to output.csv");
	} catch (error) {
		console.error("Error:", error);
	}
};

// 메인 함수 실행
main();
