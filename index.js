const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const XLSX = require('xlsx');

const sourceBucket = 'mint-transactions';
const destinationBucket = 'mint-transformed';

exports.handler = async () => {
    try {
        // List all objects in the source bucket
        const objects = await s3.listObjectsV2({ Bucket: sourceBucket }).promise();
        for (let object of objects.Contents) {
            await transformAndCopyFile(object.Key);
        }
        console.log("All files processed.");
    } catch (error) {
        console.error('Error processing files:', error);
    }
}

const transformAndCopyFile = async (fileName) => {
    try {
        // Get the file from the source bucket
        console.log("File is " + fileName);
        const file = await s3.getObject({ Bucket: sourceBucket, Key: fileName }).promise();
        const fileContents = file.Body.toString('utf-8');

        const csvWorksheet = XLSX.read(fileContents, { type: 'string' });

        // Transform the file content here as needed
        const workbook = XLSX.utils.book_new();
        const worksheet = XLSX.utils.aoa_to_sheet([[]]);


        // Define a named style for currency formatting (not applied in this code)
        // const currencyStyle = { numFmt: '$#,##0.00' };

        // Write the transformed headers to the Excel sheet
        const headers = ['', 'Item', 'Date', 'Paid By', 'Amount (CAD)', 'CherryOwesCass', 'CassOwesCherry', 'Split 50/50', 'Category'];
        XLSX.utils.sheet_add_aoa(worksheet, [headers], { origin: 'A1' });

        // Perform transformations and write to Excel sheet
        let rowIndex = 2;
        XLSX.utils.sheet_to_json(csvWorksheet, { header: 'A', raw: false }).forEach((row) => {
            if ([
                'Transfer',
                'Deposit',
                'Credit Card Payment',
                'Hide from Budgets & Trends',
                'Bank Fee',
                'Income',
                'Interest Income',
                'Mortgage & Rent',
                'Parking',
                'TFSA Investment',
                'Subscriptions',
                'Mobile Phone',
                'Books',
                'Video Games',
                'Canada Student Loan',
                'Alberta Student Loan',
            ].includes(row.Category)) {
                return;
            }

            const item = row.Description;
            const date = new Date(row.Date).toLocaleDateString();
            const paidBy = 'Cassady';
            const amountCAD = parseFloat(row.Amount);
            const cherryOwesCass = `=E${rowIndex}/2`; // Referencing Amount (CAD) cell
            const cassOwesCherry = '';
            const split5050 = `=E${rowIndex}/2`; // Referencing Amount (CAD) cell
            const category = row.Category;

            rowIndex += 1;
            XLSX.utils.sheet_add_aoa(worksheet, [[, item, date, paidBy, amountCAD, cherryOwesCass, cassOwesCherry, split5050, category]], { origin: `A${rowIndex}` });
        });

        XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
        const buffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'buffer' });

        // Upload the buffer to S3
        const newFileName = `${fileName.replace('_transactions.csv', '_transformed.xlsx')}`
        const s3UploadParams = {
            Bucket: destinationBucket,
            Key: `${newFileName}`, // Specify the S3 path
            Body: buffer
        };

        s3.putObject(s3UploadParams, (err, data) => {
            if (err) {
                console.error('Error uploading file to S3:', err);
            } else {
                console.log(`Data from ${fileName} transformed to ${newFileName} and uploaded to S3: ${data.Location}`);
            }
        });


        console.log(`File ${newFileName} copied successfully to ${destinationBucket}`);
    } catch (error) {
        console.error(`Error copying file ${fileName}:`, error);
        throw new Exception(`Error copying file ${fileName}:`, error);
    }
};