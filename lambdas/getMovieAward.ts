import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { Movie, MovieCast } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    QueryCommand,
    QueryCommandInput,
    ScanCommand,
    QueryCommandOutput
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";


const ddbDocClient = createDDbDocClient();
const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
    schema.definitions["MovieQueryParams"] || {}
  );

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {
        // Print Event
        console.log("Event: ", JSON.stringify(event));
        const parameters = event?.pathParameters;
        const min = parameters?.min;
        const awardBody = parameters?.awardBody
        const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;
        if (!movieId) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing movie Id" }),
            };
        }
        if (!awardBody) {
            return{
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing award Body" }),
            }
        }
        let filterExpression = "";
        let expressionAttributeValues: { [key: string]: { N: string } } = {
            ":movieId": { N: movieId.toString() },
            ":awardBody": { N: awardBody.toString() }
        };
        if (min) {
            filterExpression += "numAwards >= :min";
            expressionAttributeValues[":min"] = { N: min };
        }

        const queryCommandInput: {
            TableName: string | undefined;
            KeyConditionExpression: string;
            ExpressionAttributeValues: { [key: string]: { N: string } };
            FilterExpression?: string;
        } = {
            TableName: process.env.REVIEWS_TABLE_NAME,
            KeyConditionExpression: "movieId = :movieId AND awardBody = :awardBody",
            ExpressionAttributeValues: expressionAttributeValues,
            FilterExpression: filterExpression.length > 0 ? filterExpression : undefined,
        };

        if (filterExpression.length > 0) {
            queryCommandInput.FilterExpression = filterExpression;
        }

        const commandOutput = await ddbDocClient.send(new QueryCommand(queryCommandInput));

        // 返回评论者名称查询结果
        return handleQueryResponse(commandOutput);
        
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
}

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}

function handleQueryResponse(commandOutput : QueryCommandOutput) {
    if (!commandOutput.Items || commandOutput.Items.length === 0) {
        return {
            statusCode: 404,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ message: "No reviews found" }),
        };
    }

    const body = {
        data: commandOutput.Items,
    };

    return {
        statusCode: 200,
        headers: {
            "content-type": "application/json",
        },
        body: JSON.stringify(body),
    };
}