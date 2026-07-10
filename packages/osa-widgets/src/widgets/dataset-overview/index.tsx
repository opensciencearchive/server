import type { DatasetList } from "../../lib/types";
import { mountWidget } from "../mount";
import { DatasetOverview } from "./DatasetOverview";

mountWidget<DatasetList>((data, host) => <DatasetOverview data={data} host={host} />);
