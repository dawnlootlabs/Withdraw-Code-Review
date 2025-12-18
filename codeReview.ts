import { createDynamoDbDocumentClient } from "@boxed-backend/core/utils/dynamodb";
import { monotonicFactory } from "node_modules/ulidx/dist/ulid";
import { TransactWriteCommandInput } from "@aws-sdk/lib-dynamodb";

const SHIPPING_MAX_ITEMS = 15;
const dynamoDb = createDynamoDbDocumentClient();

/*
    This is an exercise to review the "withdraw" function.
    Code within types, enums, helpers, and dynamodb logic is generally correct, so don't focus on that.
    There are a few bugs, as well as some areas where best practices are not followed.
    Read through the code first, and ask any questions you have, and then we will talk through the code as a code review.

    The purpose of this function is to start the withdraw process for a given account and items.
    Pending orders have up to 14 items, and once they hit 15 items, they move to processing. 
    There is max 1 pending order at a given time.
*/

export default async function withdraw(account: AccountDdb, items: InventoryItemDdb[]) {
  if (items.length > 200) {
    throw new Error("Cannot withdraw that many items at once");
  }

  const orders: { tcgOrder: TcgOrderDdb; items: InventoryItemDdb[] }[] = [];

  const accountId = account.pk;
  const date = new Date();
  const ulid = monotonicFactory();

  const pendingOrder = await getPendingOrder(account.pk);

  const params: TransactWriteCommandInput = {
    TransactItems: [],
  };

  const processingOrderIds: string[] = [];
  if (pendingOrder) {
    const len = pendingOrder.items.length;

    const remainder = len - SHIPPING_MAX_ITEMS;
    const iToAdd = items.slice(0, remainder);

    let status: TcgOrderStatus;
    const updateExpression =
      "SET #status = :status, updatedAt = :updatedAt, #items = list_append(#items, :orderItems)";
    if (remainder === iToAdd.length) {
      status = TcgOrderStatus.PROCESSING;
      processingOrderIds.push(pendingOrder.pk);
    } else {
      status = TcgOrderStatus.PENDING;
    }

    const inventoryItemUpdates = getInventoryItemUpdates(iToAdd, date.toISOString());

    params.TransactItems?.push(
      {
        Update: {
          TableName: "ORDER",
          Key: { pk: pendingOrder.pk },
          ConditionExpression: "attribute_exists(pk) AND #status = :expectedStatus",
          UpdateExpression: updateExpression,
          ExpressionAttributeNames: { "#status": "status", "#items": "items" },
          ExpressionAttributeValues: {
            ":status": status,
            ":orderItems": iToAdd,
            ":updatedAt": date.toISOString(),
            ":expectedStatus": TcgOrderStatus.PENDING,
          },
        },
      },
      ...inventoryItemUpdates
    );

    iToAdd.forEach((item) => {
      item.status = InventoryItemStatus.WITHDRAWING;
      item.updatedAt = date.toISOString();
    });
    pendingOrder.status = status;
    pendingOrder.updatedAt = date.toISOString();
    pendingOrder.items.push(...iToAdd);

    orders.push({ tcgOrder: pendingOrder, items: iToAdd.map((item) => item) });
  }

  const shippingAddress = account.shippingAddress;
  if (!shippingAddress) {
    throw new Error("shipping address invalid");
  }

  const itemsForNewOrders: InventoryItemDdb[][] = [];
  for (let i = 0; i < items.length; i += SHIPPING_MAX_ITEMS) {
    itemsForNewOrders.push(items.slice(i, i + SHIPPING_MAX_ITEMS));
  }

  for (const itemsForNewOrder of itemsForNewOrders) {
    const tcgOrderId = ulid(date.getTime());

    if (itemsForNewOrder.length === SHIPPING_MAX_ITEMS) {
      processingOrderIds.push(tcgOrderId);
    }

    const newTcgOrder: TcgOrderDdb = {
      pk: tcgOrderId,
      accountId: accountId,
      createdAt: date.toISOString(),
      updatedAt: date.toISOString(),
      shippingAddress,
      status: TcgOrderStatus.PENDING,
      items: itemsForNewOrder,
    };

    const inventoryItemUpdates = getInventoryItemUpdates(itemsForNewOrder, date.toISOString());

    params.TransactItems?.push(
      {
        Put: {
          TableName: "ORDER",
          Item: newTcgOrder,
          ConditionExpression: "attribute_not_exists(pk)",
        },
      },
      ...inventoryItemUpdates
    );

    itemsForNewOrder.forEach((item) => {
      item.status = InventoryItemStatus.WITHDRAWING;
      item.updatedAt = date.toISOString();
    });

    orders.push({ tcgOrder: newTcgOrder, items: itemsForNewOrder.map((item) => item) });
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
        ":status": InventoryItemStatus.WITHDRAWING,
        ":updatedAt": dateStr,
        ":expectedStatus": InventoryItemStatus.UNFULFILLED,
      },
    },
  }));
}

// gets the current open pending order for an account
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
  shippingAddress?: ShippingAddress;
};

type InventoryItemDdb = {
  pk: string;
  sk: string;
  exchangeAddTxId?: string;
  exchangeRemoveTxId?: string;
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
  WITHDRAWING = "WITHDRAWING",
  WITHDRAWN = "WITHDRAWN",
}
