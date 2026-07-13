import type { TablePage } from "../../lib/types";
import { mountWidget } from "../mount";
import { DataTable } from "./DataTable";

mountWidget<TablePage>("table", (data, host) => <DataTable initial={data} host={host} />);
