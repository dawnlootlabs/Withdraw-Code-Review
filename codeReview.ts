import { createDynamoDbDocumentClient } from "@boxed-backend/core/utils/dynamodb";
import { monotonicFactory } from "node_modules/ulidx/dist/ulid";
import { TransactWriteCommandInput } from "@aws-sdk/lib-dynamodb";

const MAX_ITEMS_PER_ORDER = 15;
const dynamoDb = createDynamoDbDocumentClient();

/*
    This is an exercise to review the "withdraw" function.
    Code within types, enums, helpers, and dynamodb logic is generally correct, so don't focus on that.
    There are a few bugs, as well as some areas where best practices are not followed.
    Read through the code first, and ask any questions you have, and then we will talk through the code as a code review.

    The goal of this function is to withdraw items from an account by adding them to orders.
    Items can either be added to an existing pending order, or new orders can be created.
    Pending orders can have up to 14 items, and once they have 15 items, they move to processing.

    The withdraw function is successful if:
     - It successfully adds all inputted items to orders
     - There is max 1 pending order at a given time per account
     - Orders with 15 items are updated to processing
*/

export default async function withdraw(account: AccountDdb, items: InventoryItemDdb[]) {
  if (items.length > 200) {
    throw new Error("Cannot withdraw that many items at once");
  }

  const accountId = account.pk;
  const orders: TcgOrderDdb[] = [];
  const date = new Date();
  const ulid = monotonicFactory();
  const params: TransactWriteCommandInput = {
    TransactItems: [],
  };

  const pendingOrder = await getPendingOrder(account.pk);

  if (pendingOrder) {
    const len = pendingOrder.items.length;

    const remainder = len - MAX_ITEMS_PER_ORDER;
    const iToAdd = items.slice(0, remainder);

    let status: TcgOrderStatus = TcgOrderStatus.PENDING;

    if (remainder == iToAdd.length) {
      status = TcgOrderStatus.PROCESSING;
    }

    pendingOrder.status = status;
    pendingOrder.updatedAt = date.toISOString();
    pendingOrder.items.push(...iToAdd);

    params.TransactItems?.push(...getInventoryItemUpdates(iToAdd, date.toISOString()));
    params.TransactItems?.push(getTcgOrderUpdate(pendingOrder));

    orders.push(pendingOrder);
  }

  const shippingAddress = account.shippingAddress;
  if (!shippingAddress) {
    throw new Error("shipping address invalid");
  }

  // split items into orders of MAX_ITEMS_PER_ORDER
  const itemsForNewOrders: InventoryItemDdb[][] = [];
  for (let i = 0; i < items.length; i += MAX_ITEMS_PER_ORDER) {
    itemsForNewOrders.push(items.slice(i, i + MAX_ITEMS_PER_ORDER));
  }

  // create new orders for each batch of items
  for (const itemsForNewOrder of itemsForNewOrders) {
    const tcgOrderId = ulid(date.getTime());

    const newTcgOrder: TcgOrderDdb = {
      pk: tcgOrderId,
      accountId: accountId,
      createdAt: date.toISOString(),
      updatedAt: date.toISOString(),
      shippingAddress,
      status: TcgOrderStatus.PENDING,
      items: itemsForNewOrder,
    };

    params.TransactItems?.push(...getInventoryItemUpdates(itemsForNewOrder, date.toISOString()));

    params.TransactItems?.push({
      Put: {
        TableName: "ORDER",
        Item: newTcgOrder,
        ConditionExpression: "attribute_not_exists(pk)",
      },
    });

    orders.push(newTcgOrder);
  }

  await dynamoDb.transactWrite(params);

  return {
    orders,
    account,
  };
}

// Helpers

// returns dynamodb update items for a list of inventory items
function getInventoryItemUpdates(items: InventoryItemDdb[], dateStr: string) {
  return items.map((item) => ({
    Update: {
      TableName: "INVENTORY",
      Key: { pk: item.pk, sk: item.sk },
      ConditionExpression: "attribute_exists(pk) AND #status = :expectedStatus",
      UpdateExpression: "SET #status = :status, updatedAt = :updatedAt",
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: {
        ":status": InventoryItemStatus.WITHDRAWN,
        ":updatedAt": dateStr,
        ":expectedStatus": InventoryItemStatus.UNFULFILLED,
      },
    },
  }));
}

// returns dynamodb update item for a tcg order
function getTcgOrderUpdate(pendingOrder: TcgOrderDdb) {
  return {
    Update: {
      TableName: "ORDER",
      Key: { pk: pendingOrder.pk },
      ConditionExpression: "attribute_exists(pk) AND #status = :expectedStatus",
      UpdateExpression: "SET #status = :status, updatedAt = :updatedAt, #items = :items",
      ExpressionAttributeNames: { "#status": "status", "#items": "items" },
      ExpressionAttributeValues: {
        ":status": pendingOrder.status,
        ":items": pendingOrder.items,
        ":updatedAt": pendingOrder.updatedAt,
        ":expectedStatus": TcgOrderStatus.PENDING,
      },
    },
  };
}

// gets the current open pending order for an account if it exists
async function getPendingOrder(accountId: string): Promise<TcgOrderDdb | undefined> {
  const result = await dynamoDb.query({
    TableName: "ORDER",
    KeyConditionExpression: "pk = :accountId",
    FilterExpression: "#status = :pending",
    ExpressionAttributeNames: {
      "#status": "status",
    },
    ExpressionAttributeValues: {
      ":accountId": accountId,
      ":pending": TcgOrderStatus.PENDING,
    },
    ScanIndexForward: false, // newest first
    Limit: 1,
  });

  return result.Items?.[0] as TcgOrderDdb | undefined;
}

// Types
type AccountDdb = {
  pk: string;
  email: string;
  shippingAddress?: ShippingAddress;
};

type InventoryItemDdb = {
  pk: string;
  sk: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  status: InventoryItemStatus;
};

type TcgOrderDdb = {
  pk: string;
  createdAt: string;
  updatedAt: string;
  accountId: string;
  status: TcgOrderStatus;
  shippingAddress: ShippingAddress;
  items: InventoryItemDdb[];
};

type ShippingAddress = {
  firstName: string;
  lastName: string;
  addressLine1: string;
  addressLine2?: string;
  locality: string;
  region: string;
  postalCode: string;
  country: string;
  phoneNumber?: string;
  countryCode?: string;
};

enum TcgOrderStatus {
  CANCELLED = "CANCELLED",
  PENDING = "PENDING",
  PROCESSING = "PROCESSING",
  SHIPPED = "SHIPPED",
}

enum InventoryItemStatus {
  UNFULFILLED = "UNFULFILLED",
  WITHDRAWN = "WITHDRAWN",
}
