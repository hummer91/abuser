const fs = require("fs");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

// CSV 파일을 읽어서 데이터를 배열로 변환하는 함수
const readCSV = async (filePath) => {
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
			{ id: "USER", title: "USER" },
			{ id: "REWARD_WP", title: "REWARD_WP" },
			{ id: "CONSUMED_STAMINA", title: "CONSUMED_STAMINA" },
			{ id: "CONSUMED_MINUTES", title: "CONSUMED_MINUTES" },
			{ id: "THUMBNAIL_DISPLAY", title: "THUMBNAIL_DISPLAY" },
			{ id: "RUNNING_TIME", title: "RUNNING_TIME" },
			{ id: "DISTANCE", title: "DISTANCE" },
			{ id: "AVG_SPEED", title: "AVG_SPEED" },
			{ id: "TOTAL_STEP_COUNT", title: "TOTAL_STEP_COUNT" },
			{ id: "FINISHED", title: "FINISHED" },
			{ id: "MODE", title: "MODE" },
			{ id: "CREATED_GMT", title: "CREATED_GMT" },
			{ id: "UPDATED_GMT", title: "UPDATED_GMT" },
			{ id: "CREATED", title: "CREATED" },
			{ id: "UPDATED", title: "UPDATED" },
		],
	});

	return csvWriter.writeRecords(data);
};

// 두 시간을 비교하여 10분 이내로 일치하는 값을 찾는 함수
const findMatchingTimes = (data1, data2) => {
	const matchedData = [];

	for (let i = 0; i < data1.length; i++) {
		const StartTime1 = new Date(data1[i].CREATED_GMT);
		const EndTime1 = new Date(data1[i].UPDATED_GMT);

		for (let j = 0; j < data2.length; j++) {
			const StartTime2 = new Date(data2[j].CREATED_GMT);
			const EndTime2 = new Date(data2[j].UPDATED_GMT);
			const diffStartTime = Math.abs(StartTime1 - StartTime2) / (1000 * 60);

			if (diffStartTime <= 30) {
				const diffEndTime = Math.abs(EndTime1 - EndTime2) / (1000 * 60);
				if (diffEndTime <= 30) {
					matchedData.push({
						USER: data1[i].USER,
						REWARD_WP: data1[i].REWARD_WP,
						CONSUMED_STAMINA: data1[i].CONSUMED_STAMINA,
						CONSUMED_MINUTES: data1[i].CONSUMED_MINUTES,
						THUMBNAIL_DISPLAY: data1[i].THUMBNAIL_DISPLAY,
						RUNNING_TIME: data1[i].RUNNING_TIME,
						DISTANCE: data1[i].DISTANCE,
						AVG_SPEED: data1[i].AVG_SPEED,
						TOTAL_STEP_COUNT: data1[i].TOTAL_STEP_COUNT,
						FINISHED: data1[i].FINISHED,
						MODE: data1[i].MODE,
						CREATED_GMT: data1[i].CREATED_GMT,
						UPDATED_GMT: data1[i].UPDATED_GMT,
						CREATED: data1[i].CREATED,
						UPDATED: data1[i].UPDATED,
					});
					matchedData.push({
						USER: data2[j].USER,
						REWARD_WP: data2[j].REWARD_WP,
						CONSUMED_STAMINA: data2[j].CONSUMED_STAMINA,
						CONSUMED_MINUTES: data2[j].CONSUMED_MINUTES,
						THUMBNAIL_DISPLAY: data2[j].THUMBNAIL_DISPLAY,
						RUNNING_TIME: data2[j].RUNNING_TIME,
						DISTANCE: data2[j].DISTANCE,
						AVG_SPEED: data2[j].AVG_SPEED,
						TOTAL_STEP_COUNT: data2[j].TOTAL_STEP_COUNT,
						FINISHED: data2[j].FINISHED,
						MODE: data2[j].MODE,
						CREATED_GMT: data2[j].CREATED_GMT,
						UPDATED_GMT: data2[j].UPDATED_GMT,
						CREATED: data2[j].CREATED,
						UPDATED: data2[j].UPDATED,
					});
				}
			}
		}
	}
	return matchedData;
};

// 메인 함수
const main = async () => {
	const numFiles = parseInt(process.argv[3]);

	let inputDataArray = [];

	for (let i = 1; i <= numFiles; i++) {
		const inputData = await readCSV(`ex${process.argv[2]}/${i}.csv`);
		inputDataArray.push(inputData);
	}

	const matchedData = findMatchingTimes(inputDataArray[0], inputDataArray[1]);

	let totalRows1 = inputDataArray[0].length;
	let totalRows2 = inputDataArray[1].length;
	const totalRows = totalRows1 + totalRows2;

	const matchedRows = matchedData.length;

	const ratio = (matchedRows / totalRows) * 100;

	matchedData.push({ USER: `10 Minute Match Ratio: ${ratio.toFixed(2)}%` });

	const outputFilePath = `ex${process.argv[2]}/output${process.argv[3]}.csv`;

	await writeCSV(outputFilePath, matchedData);

	console.log(`Matching times saved to ${outputFilePath}`);
};

// 메인 함수 실행
main().catch((error) => {
	console.error("Error:", error);
});
