import type { RecordDetailData } from "../../lib/types";
import { mountWidget } from "../mount";
import { RecordDetail } from "./RecordDetail";

mountWidget<RecordDetailData>("record", (data, host) => <RecordDetail data={data} host={host} />);
